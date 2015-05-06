var Q = require('q');

var Queue = module.exports = function(name, opts) {
    this.name = name || opts.name;

    this.nats = require('./nats').connect({
        url: opts.servers || 'nats://localhost:4222',
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
        try {
            var info = JSON.parse(response);
            if (info.error) {
                deferred.reject(info.error);
            } else
            if (info.progress) {
                deferred.notify(info.progress);
            } else {
                deferred.resolve(info.response);
            }
        }
        catch (err) {
            deferred.reject(err);
        }
    });

    return deferred.promise;
};

Queue.prototype.process = function(opts, handler) {
    var self = this;
    if (!handler) {
        handler = opts;
        opts = {};
    }

    var subscribe = function(workerId) {
        self.nats.subscribe(self.name, {queue:self.name, max: 1}, function(request, replyTo) {

            var data = JSON.parse(request);

            var job = {
                request:data,
                progress: function(progressInfo) {
                    self.nats.publish(replyTo, JSON.stringify({
                        progress: progressInfo
                    }));
                }
            };

            handler(job, function(err, response) {
                // subscribe again to handle another job
                subscribe(workerId);

                if (err) {
                    self.nats.publish(replyTo, JSON.stringify({
                        error: err
                    }));
                    return;
                }

                self.nats.publish(replyTo, JSON.stringify({
                    response: response
                }));
            });

        });
    }

    var totalWorkers = opts.workers || 1;
    for (var pos=0; pos<totalWorkers; pos++) {
        subscribe(pos);
    }
}
