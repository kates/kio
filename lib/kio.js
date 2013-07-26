//var cluster = require('cluster'),
var redis = require('redis');
var async = require('async');

var Kio = function(options, callback) {
  var running = false;
  var workers = [];
  var name = options['name'];
  var retries = parseInt(options["retries"]) || 0;
  var redis_client = redis.createClient(options['redis']);
  var verbose = !!options['verbose'] || true;
  var namespace = options['namespace'] ? options['namespace'] + ":" : "";
  var DEFERRED = namespace + "kio:deferred:";
  var DEFERRED_JOBS = DEFERRED + "jobs:" + name + ":";
  var DEFERRED_INDEX = DEFERRED + "index:" + name;
  var QUEUE = namespace + "kio:queue:";
  var QUEUE_JOBS = QUEUE + "jobs:" + name;
  var QUEUE_ACTIVE = QUEUE + "active:" + name;

  if (typeof name != 'string') {
    throw "name must me be a string!";
  }

  var Worker = function(worker_id) {
    this.worker_id = worker_id;
    Worker.prototype.work = function(job, cb) {
      var that = this;
      try {
        callback(function(err) {
          if (!err) {
            cb(null, job);
          }
          workers.unshift(that);
        }, job['args'], that.worker_id);
      } catch(err) {
        if (verbose) {
          console.log("exception caught: ", err);
        }
        cb(err, job, that.worker_id);
          workers.unshift(that);
      }
    };
  };

  var doLoop = function() {
    process.nextTick(function() {
      loops();
    });
  };

  var processDelayed = function() {
    redis_client.smembers(DEFERRED_INDEX, function(err, members) {
      var ms = new Date().getTime();
      for (var i = 0; i < members.length; i++) {
        var index = parseInt(members[i]);
        if (index <= ms) {
          var deferreds = DEFERRED_JOBS + index;
          var jobs = redis_client.llen(deferreds);
          while(jobs-- > 0) {
            redis_client.rpoplpush(deferreds, QUEUE_JOBS, function(err) {});
          }
          redis_client.srem(DEFERRED_INDEX, index, function(err) {});
        }
      }
    });
  };

  var processQueue = function() {
    redis_client.brpoplpush(QUEUE_JOBS, QUEUE_ACTIVE, 1, function(err, job) {
      if (!job) {
        doLoop();
        return;
      }

      if (!running) {
        redis_client.lpush(QUEUE_JOBS, job);
        return;
      }

      var worker = workers.pop();

      if (!worker) {
        redis_client.lpush(QUEUE_JOBS, job);
        workers.unshift(worker);
        doLoop();
        return;
      }

      worker.work(JSON.parse(job), function(err, job) {
        if (err) {
          redis_client.lrem(QUEUE_ACTIVE, 1, JSON.stringify(job));
          if (job['retries']-- > 0) {
            redis_client.lpush(QUEUE_JOBS, JSON.stringify(job));
          }
        } else {
          redis_client.lrem(QUEUE_ACTIVE, 1, JSON.stringify(job));
        }
      });

      if (running) {
        doLoop();
      }
    });
  };

  var loops = function() {
    processDelayed();
    processQueue();
  };

  var createJob = function(job) {
    return JSON.stringify({
      retries: retries,
      args: job,
      id: Math.ceil(Math.random() * 10000000000000000)
    });
  };

  var enqueue = function(job) {
    redis_client.lpush(QUEUE_JOBS, createJob(job));
  };

  Kio.prototype.enqueue = enqueue;

  Kio.prototype.defer = function(job, ms) {
    ms = ms || 1000;
    var date = new Date();
    date.setTime(date.getTime() + ms);
    var time = date.getTime();
    redis_client.lpush(DEFERRED_JOBS + time, createJob(job));
    redis_client.sadd(DEFERRED_INDEX, time);
  };

  Kio.prototype.start = function(concurrency) {
    concurrency = concurrency || 1;
    running = true;
    for (var i = 0; i < concurrency; i++) {
      workers.unshift(new Worker(i));
      if (verbose) {
        console.log("started worker " + i);
      }
    }
    
    process.on('exit', function() {
      running = false;
    });

    process.on("uncaughtException", function() {
      console.warn("uncaught exception", arguments);
      doLoop();
    });
    doLoop();
  };

  Kio.prototype.stop = function() {
    running = false;
  };

  Kio.prototype.quit = function() {
    this.stop();
    redis_client.quit();
  };
  Kio.prototype.close = Kio.prototype.quit;
};

module.exports.Kio = Kio;

/*
if (cluster.isMaster) {
  var workers = [];
  for (var i = 0; i < 4; i++) {
    workers.push(cluster.fork());
  }
  process.on('exit', function(){
    console.log("master exiting");
  });
  process.on("SIGINT", function() {
    console.log("SIGINT. Ctrl-D to exit");
    for(var i = 0; i < workers.length; i++) {
      workers[i].kill();
    }
    setTimeout(function() {
      process.exit();
    }, 2000);
  });
} else if (cluster.isWorker) {
  process.on('exit', function(){
    console.log("worker exiting");
  });
}
*/

