var Queue   = require('../../index');

var queueName = process.env.QUEUE_NAME || 'qname';

var options = {
    workers: 10,
    servers: [
        'nats://192.168.59.103:4222',
        'nats://192.168.59.103:4223'
    ]
};

var queue = new Queue(queueName, options);

var count = 1;
var createRequest = function() {
   var id = count++;
   console.log('Creating Request ' + id);

    var request = {
        id:id
    }
    queue.request(request)
        .then(function(response) {
            console.log('RESPONSE >>> ' + JSON.stringify(response));
        })
        .progress(function(message) {
            console.log('PROGRESS >>> ' + message);

        })
        .catch(function(err) {
            console.error('ERROR >>> ' + err);
        });
}

setInterval(createRequest, 1000);

console.log('Queue server - ' + queueName);
