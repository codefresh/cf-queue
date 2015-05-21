var Queue   = require('../../index');

var queueName = process.env.QUEUE_NAME || 'qname';
var workerName = process.env.WORKER_NAME || 'worker';
var options = {
    workers: 10,
    servers: [
        'nats://192.168.59.103:4222',
        'nats://192.168.59.103:4223'
    ]
};

var queue = new Queue(queueName, options);

console.log('Worker - ' + workerName);
queue.process(function(job, callback) {
    var request = job.request;

    console.log('Got request >>> ' + JSON.stringify(request));

    var sendProgress = function(step) {
        return function() {
            job.progress(step);
        }
    }

    setTimeout(sendProgress('Step 1'), 500);
    setTimeout(sendProgress('Step 2'), 1000);
    setTimeout(sendProgress('Step 3'), 1500);
    setTimeout(sendProgress('Step 4'), 2000);
    setTimeout(function() {
        var response = {
            id: request.id
        }
        callback(null, response);
    }, 2500);

});