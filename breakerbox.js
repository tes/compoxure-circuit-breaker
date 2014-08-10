// CircuitBreaker
// ==============
// 
// Core of this code was taken from https://github.com/yammer/circuit-breaker-js
// Decision was taken to re-implement rather than fork, as this version will not 
// be designed to work on the client.
//
// Hystrix-like circuit breaker for JavaScript.
var uuid = require('node-uuid');
var redis = require('redis');
var url = require('url');
var _ = require('lodash');
var util = require('util');
var events = require("events");
var Warlock = require('node-redis-warlock');
var CircuitBreaker = require('./circuitbreaker');

module.exports = BreakerBox;

function BreakerBox(opts, next) {

  var self = this;

  opts = opts || {};

  this.guid            = uuid.v4();
  this.key             = opts.key || "unknown";
  this.tickerInterval  = opts.tickerInterval || 1000;
  this.lockTTL         = opts.lockTTL || 10000;
  this.redisUrl        = opts.redisUrl || "redis://localhost:6379";

  this._redisOptions   = parseRedisConnectionString(this.redisUrl);
  this._circuits       = {};

  this._channel        = ["breaker", this.key].join(":");
  this._lockkey        = ["breaker", this.key, "lock"].join(":");
  this._circuitlist    = ["breaker", this.key, "circuits"].join(":");

  this._initialise(function(err) {
    if(err) return next(err);
    self._listen();    
    self._startTicker();
    next();
  });

  events.EventEmitter.call(this);

}

util.inherits(BreakerBox, events.EventEmitter);

BreakerBox.prototype._listen = function() {
  var self = this,
      rs = self._redisSubscriberClient
      channel = self._channel,
      circuitRegExp = new RegExp(channel + ':circuit:.*');

  rs.psubscribe(channel + '*');

  rs.on('pmessage', function(pattern, messageChannel, data) {

    if(messageChannel == channel) {
      self._loadCircuit(data);
    }

    if(circuitRegExp.test(messageChannel)) {
      var data = JSON.parse(data);
      self._updateCircuit(data.circuitName, data);
    }

  });

}

BreakerBox.prototype._startTicker = function() {

  var self = this;
  
  var tick = function() {

    self.emit("checklock",{guid:self.guid,pid:process.pid});

    self._getLock(function(err, unlock) {

      if(!unlock) return; // Don't have lock

      self.emit("tick",{guid:self.guid,pid:process.pid});
      self._tickAllCircuits();

      if(err) {
        self.emit("error",{error:error,guid:self.guid,pid:process.pid});
        return;
      }
      
      setTimeout(function() {
        self.emit('unlock',{guid:self.guid,pid:process.pid});
        unlock();
      }, self.tickerInterval);

    });

  };

  setInterval(tick, self.tickerInterval);

};

BreakerBox.prototype._tickAllCircuits = function() {
  var self = this;
  _.forOwn(self._circuits, function(circuit, key) {
    if(typeof circuit.tick == 'function') {
      circuit.tick();
    }
  });
}

BreakerBox.prototype.CircuitBreaker = function(opts, next) {

  var self = this;
  if(!opts.circuitName) self.emit('error',{message:'You must provide a unique name for the circuit.'});
  self._circuits[opts.circuitName] = self._circuits[opts.circuitName] || new CircuitBreaker(opts, self);
  next(null, self._circuits[opts.circuitName]);

};

BreakerBox.prototype._loadCircuit = function(name) {

  var self = this;
  if(self._circuits[name]) return;

  self._getCircuit(name, function(err, opts) {

    if(err || !opts) return;
    opts.loadOnly = true;
    self._circuits[opts.circuitName] = new CircuitBreaker(opts, self);

  });
  
};


BreakerBox.prototype._updateCircuit = function(name, opts) {

  var self = this, circuit = self._circuits[name];
  if(!circuit) return;
  _.merge(circuit, opts);
  
};

BreakerBox.prototype._getCircuit = function(name, next) {

  var self = this, 
      r = self._redisClient, 
      key = ["breaker", self.key, "circuit", name].join(":"),
      next = next || function() {};

  r.hgetall(key, function(err, data) {
    next(err, data);
  });

}

BreakerBox.prototype._getLock = function(next) {
  var self = this, key = self._lockkey, ttl = self.lockTTL;
  self._lock.lock(key, ttl, function(err, unlock){
    if (err) return next(err);
    next(null, unlock);
  });
}

BreakerBox.prototype._initialise = function(next) {
  var self = this;
  self._redisClient = redis.createClient(self._redisOptions.port, self._redisOptions.host);
  self._redisClient.select(self._redisOptions.db || 0);
  self._redisSubscriberClient = redis.createClient(self._redisOptions.port, self._redisOptions.host);
  self._redisSubscriberClient.select(self._redisOptions.db || 0);
  self._lock = Warlock(self._redisClient);
  setImmediate(next);
};

function parseRedisConnectionString(connectionString) {
    var params = url.parse(connectionString, true);
    return {
        host: params.hostname,
        port: params.port && parseInt(params.port) || 6379,
        db: params.query.db && parseInt(params.query.db) || 0
    };
}