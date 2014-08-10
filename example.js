var cluster = require('cluster');
var BreakerBox = require('./breakerbox');

if (cluster.isMaster) {
	cluster.fork();
	cluster.fork();
	cluster.fork();
	cluster.fork();
} else {

	var bb = new BreakerBox({key:'test'}, ready), variant = 1;

	bb.on('tick', function(data) {
	    //console.log(data.pid + ": Circuit breaker tick");
	})

	bb.on('checklock', function(data) {
	    //console.log(data.pid + ": Check Lock");
	})

	bb.on('lock', function(data) {
	    //console.log(data.pid + ": Lock");
	})

	bb.on('unlock', function(data) {
	    //console.log(data.pid + ": Unlock");
	})

	bb.on('open', function(circuit, data) {
		console.log('OPEN ' + process.pid + ": " + circuit + " " + data.state);
	})

	bb.on('closed', function(circuit, data) {
		console.log('CLOSED ' + process.pid + ": " + circuit + " " + data.state)
		variant = 1;
	});

	function ready() {
  		
  		var mod = process.pid % 2;

		bb.CircuitBreaker({circuitName:'alpha'}, function(err, cb) {

			setInterval(function() {
				cb.run(function(success, failure) {
					if(variant % 2 == 1) { failure() } else { success() };	
				}, function() {					
					variant = 2;
				});
			},200);

		});			

	    console.log("Circuit breaker initialised");
		
	}

	function loop() {
		setTimeout(loop,1000);
	}
	loop();
}