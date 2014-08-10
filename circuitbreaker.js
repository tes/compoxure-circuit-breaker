// CircuitBreaker
// ==============
// 
// Core of this code was taken from https://github.com/yammer/circuit-breaker-js
// Decision was taken to re-implement rather than fork, as this version will not 
// be designed to work on the client.
//
// Hystrix-like circuit breaker for JavaScript.
var uuid = require('node-uuid');
var url = require('url');
var _ = require('lodash');
var util = require('util');
var events = require("events");
var async = require('async');

/**
 * CircuitBreaker
 */
function CircuitBreaker(opts, breakerbox, next) {

  var self = this;

  opts = opts || {};

  this.circuitName     = opts.circuitName     || "";
  this.windowDuration  = opts.windowDuration  || 10000; // milliseconds
  this.numBuckets      = opts.numBuckets      || 10;    // number
  this.timeoutDuration = opts.timeoutDuration || 3000;  // milliseconds
  this.errorThreshold  = opts.errorThreshold  || 50;    // percentage
  this.volumeThreshold = opts.volumeThreshold || 50;     // number
  this.baseTime        = new Date().getTime();

  this._bucketDuration = this.windowDuration / this.numBuckets;
  
  this.onCircuitOpen   = opts.onCircuitOpen   || function() {};
  this.onCircuitClose  = opts.onCircuitClose  || function() {};

  this.state           = opts.state           || CircuitBreaker.CLOSED;

  this._defaults       = { failures: 0, successes: 0, timeouts: 0, shortCircuits: 0 };
  this._breakerbox     = breakerbox;
  this._redisClient    = breakerbox._redisClient;
  this._channel        = breakerbox._channel;
  this._lastCommand    = new Date();

  this._key = ["breaker", self._breakerbox.key, "circuit", self.circuitName].join(":");

  if(!opts.loadOnly) this._persist(next);

  events.EventEmitter.call(this);

};

util.inherits(CircuitBreaker, events.EventEmitter);

CircuitBreaker.OPEN = "OPEN";
CircuitBreaker.HALF_OPEN = "HALF_OPEN";
CircuitBreaker.CLOSED = "CLOSED";

CircuitBreaker.prototype._persist = function(next) {

  var self = this, 
      r = self._redisClient, 
      key = self._key,
      channel = self._channel,
      next = next || function() {};

  var data = {
    circuitName: self.circuitName,
    windowDuration: self.windowDuration,
    numBuckets: self.numBuckets,
    timeoutDuration: self.timeoutDuration,
    errorThreshold: self.errorThreshold,
    volumeThreshold: self.volumeThreshold,
    state: self.state
  };

  r.hmset(key, data, function(err) {
    r.sadd(self._breakerbox._circuitlist, self.circuitName, function(err) {
      r.publish(channel, self.circuitName, next);  
    });
  });

}

CircuitBreaker.prototype._setState = function(state, next) {

  var self = this, 
      r = self._redisClient, 
      key = self._key,
      change,
      bb = self._breakerbox,
      channel = [self._channel,"circuit",self.circuitName].join(":");

  self.state = state;

  r.hset(key, 'state', state, function(err) {
      r.publish(channel, JSON.stringify({circuitName: self.circuitName, state: state}), function(err) {
          if(state == CircuitBreaker.OPEN) bb.emit('open', self.circuitName, {circuitName: self.circuitName, state: state});
          if(state == CircuitBreaker.CLOSED) bb.emit('closed', self.circuitName, {circuitName: self.circuitName, state: state});
      });  
  });

}

CircuitBreaker.prototype.tick = function() {

  var self = this,
      r = self._breakerbox._redisClient,
      tick = [self._key,"ticker"].join(""),
      bbchannel = self._breakerbox._channel;

  r.get(tick, function(err, ts) {
    
    // If we have a tick on Redis, always trust that (as we may have just come to life)
    // This should baseline them all around the service that did the first tick
    if(ts) {
      self.baseTime = +ts;
    }

    var isNewTick = (new Date().getTime()) - self.baseTime > self._bucketDuration;
    
    if(isNewTick) {

      self.baseTime = +self.baseTime + self._bucketDuration;

      r.set(tick, self.baseTime, function(err) {

        self._createBucket(function(err) {

          self.state = CircuitBreaker.HALF_OPEN;          
          r.publish(self._key, JSON.stringify({circuitName: self.circuitName, baseTime: self.baseTime}), function(err) {});  

        });
        
      });

    };

    // Publish the channel to allow all the other nodes to load it if they don't have it
    r.publish(bbchannel, self.circuitName);  

  });
  
}

CircuitBreaker.prototype._createBucket = function(next) {
  
  var self = this,
      r = self._breakerbox._redisClient,
      bucket = [self._key, "bucket", self.baseTime].join(":"),
      channel = self._breakerbox._channel;

    r.hmset(bucket, self._defaults, function(err) {
      r.expire(bucket, (2 * self.windowDuration)/1000, next);
    });

};

CircuitBreaker.prototype._getBucketKey = function(offset) {
    var self = this, baseTime = self.baseTime;
    if(offset) baseTime = baseTime + (offset * self._bucketDuration);
    return [self._key, "bucket", baseTime].join(":");
}

CircuitBreaker.prototype._lastBucket = function(next) {
  var self = this;
  return bucket = self._getBucketKey(-1);
};

CircuitBreaker.prototype._executeCommand = function(command) {

  var self = this,
      r = self._breakerbox._redisClient,
      timeout;

  var increment = function(prop) {
    return function() {
      
      if (!timeout) { return; }

      var bucket = self._getBucketKey();

      r.exists(bucket, function(err, exists) {
        if(!exists) {
          self._createBucket(function(err) {
            r.hincrby(bucket, prop, 1); 
          });
        } else {
          r.hincrby(bucket, prop, 1); 
        }
      })

      if (self._forced == null) {
        self._updateState();
      }

      clearTimeout(timeout);
      timeout = null;
    };
  };

  timeout = setTimeout(increment('timeouts'), this.timeoutDuration);

  command(increment('successes'), increment('failures'));

};

CircuitBreaker.prototype._executeFallback = function(fallback) {

   var self = this,
      r = self._breakerbox._redisClient;

  fallback();

  var bucket = self._lastBucket();

  r.exists(bucket, function(err, exists) {
    if(!exists) {
      self._createBucket(function(err) {
        r.hincrby(bucket, 'shortCircuits', 1); 
      });
    } else {
      r.hincrby(bucket, 'shortCircuits', 1); 
    }
  });

};

CircuitBreaker.prototype._calculateMetrics = function(next) {

  var self = this,
      r = self._breakerbox._redisClient,
      errors, errorstotalCount = 0, errorCount = 0, totalCount = 0, errorPercentage = 0, lastSuccesses = 0;

  async.times(this.numBuckets, function(n, cb) {
    
    var bucket = self._getBucketKey(-1*n);
    r.hgetall(bucket, function(err, data) {
      if(data) {
        data.successes = +data.successes;
        data.failures = +data.failures;
        data.timeouts = +data.timeouts;
        data.shortCircuits = +data.shortCircuits;
      } else {
        data = self._defaults;
      }
      cb(err, data);
    });

  }, function(err, metrics) {

    for (var i = 0, l = metrics.length; i < l; i++) {
      if(metrics[i]) {
        errors = (metrics[i].failures + metrics[i].timeouts);
        errorCount += errors;
        totalCount += (errors + metrics[i].successes);
        errorPercentage = (errorCount / (totalCount > 0 ? totalCount : 1)) * 100;
      } else {
        if(metrics[i-1]) {
          lastSuccesses = metrics[i-1].successes;
        }
      }
    }

    next(null, { lastSuccesses: lastSuccesses, totalCount: totalCount, errorCount: errorCount, errorPercentage: errorPercentage })

  })

};

CircuitBreaker.prototype._updateState = function() {

  var self = this;

  var metrics = self._calculateMetrics(function(err, metrics) {

    console.log(metrics);

    if (self.state == CircuitBreaker.HALF_OPEN) {

      var lastCommandFailed = !metrics.lastSuccesses && metrics.errorCount > 0;

      if (lastCommandFailed) {        
        self._setState(CircuitBreaker.OPEN);
      } else {
        self._setState(CircuitBreaker.CLOSED);
      }
    } else {

      var overErrorThreshold = metrics.errorPercentage > self.errorThreshold;
      var overVolumeThreshold = metrics.totalCount > self.volumeThreshold;
      var overThreshold = overVolumeThreshold && overErrorThreshold;

      if (overThreshold) {
        self._setState(CircuitBreaker.OPEN);
      }

    }

  });

};


CircuitBreaker.prototype.run = function(command, fallback) {
  if (this.isOpen()) {
    this._executeFallback(fallback || function() {});
  }
  else {
    this._executeCommand(command);
  }
};

CircuitBreaker.prototype.forceClose = function() {

  this._forced = this.state;
  this._setState(CircuitBreaker.CLOSED);

};

CircuitBreaker.prototype.forceOpen = function() {
  this._forced = this.state;
  this._setState(CircuitBreaker.OPEN);
};

CircuitBreaker.prototype.unforce = function() {
  this.state = this._forced;
  this._forced = null;
};

CircuitBreaker.prototype.isOpen = function() {
  return this.state == CircuitBreaker.OPEN;
};

module.exports = CircuitBreaker;