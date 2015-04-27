var Q = require('q');

var Queue = module.exports = function(name, opts) {
    this.name = name || opts.name;

    this.nats = require('./nats').connect({
        url: opts.url || 'nats://localhost:4222',
        reconnect: true,
        maxReconnectAttempts: 10,
        reconnectTimeWait: 500
    });

    this.nats.on('error', function(err) {
        console.error(err);
    });
};

Queue.prototype.request = function(data) {
    var deferred = Q.defer();

    var payload = data;
    if ('object' === typeof data) {
        payload = JSON.stringify(data);
    }

    this.nats.request(this.name, payload, function(response) {
        var info = JSON.parse(response);
        if (info.error) {
            deferred.reject(info.error);
        } else
        if (info.progress) {
            deferred.notify(info.progress);
        } else {
            deferred.resolve(info.response);
        }
    });

    return deferred.promise;
};

Queue.prototype.process = function(handler) {
    var nats = this.nats;

    nats.subscribe(this.name, {'queue':this.name}, function(request, replyTo) {

        var data = JSON.parse(request);

        var job = {
            request:data,
            progress: function(progressInfo) {
                nats.publish(replyTo, JSON.stringify({
                    progress: progressInfo
                }));
            }
        };

        handler(job, function(err, response) {
            if (err) {
                nats.publish(replyTo, JSON.stringify({
                    error: err
                }));
                return;
            }

            nats.publish(replyTo, JSON.stringify({
                response: response
            }));
        });

    });
}