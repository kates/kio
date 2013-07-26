var Kio = require("../lib/kio").Kio;

var Worker = new Kio({name: "crawler", retries: 2}, function(done, job, worker_id) {
  var timeout = Math.ceil(Math.random() * 10000); // simulate an lengthy process
  console.log("Worker " + worker_id + " doing a " + job);
  setTimeout(function() {
    console.log("Worker " + worker_id + " finished " + job);
    done();
  }, timeout);
});

module.exports.Worker = Worker;
