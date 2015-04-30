var Queue   = require('cf-queue');

var queueName = process.env.QUEUE_NAME || 'qname';

var options = {
    servers: [
        'nats://gnats:4222'
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

setInterval(createRequest, 4000);

console.log('Queue server - ' + queueName);
