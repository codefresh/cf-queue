var Q = require('q'),
    domain = require('domain');
    CFError = require('cf-errors');

//TODO move to some error-utilities...
//credit: http://stackoverflow.com/questions/18391212/is-it-not-possible-to-stringify-an-error-using-json-stringify/18391400#18391400
var jsonifyError = function(err, filter, space) {
    var plainObject = {};
    Object.getOwnPropertyNames(err).forEach(function(key) {
        plainObject[key] = err[key];
    });
    return JSON.stringify(plainObject, filter, space);
};

var Queue = module.exports = function(name, opts) {
    this.name = name || opts.name;

    // default timeout of 30sec for someone to start handling the request
    this.timeout = opts.timeout || 10*1000;

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
    var self = this;
    var deferred = Q.defer();
    var timedout = false;

    var payload = data;
    if ('object' === typeof data) {
        payload = JSON.stringify(data);
    }

    var tid = setTimeout(function() {
        timedout = true;
        deferred.reject(new Error('timeout, no worker got the job'));
    }, self.timeout);

    var kid;

    // create keep alive timeout mechanism
    var keepAlive = function() {
        if (kid) {
            clearTimeout(kid);
        }

        // if we already got timeout we don't want to recreate keep alive
        if (timedout) {
            return;
        }

        kid = setTimeout(function() {
            timedout = true;
            deferred.reject(new Error("keep alive, didn't got any response from worker"));
        }, self.timeout * 2);
    };

    var stopKeepAlive = function() {
        if (kid) {
            clearTimeout(kid);
            kid = null;
        }
    };

    keepAlive();

    this.nats.request(this.name, payload, function(response) {
        try {
            // on any response from the worker we reset the keep alive
            keepAlive();

            // if this request already timed out, we don't want to handle it
            if (timedout) {
                return;
            }

            var info = JSON.parse(response);

            // if worker got the request we can stop the error timeout
            if (info.status === 'received') {
                if (tid) {
                    clearTimeout(tid);
                    tid = null;
                }
                return;
            }

            if (info.status === 'keep-alive') {
                return;
            }

            if (info.status === 'started') {
                return;
            }

            if (info.error) {
                stopKeepAlive();
                deferred.reject(info.error);
            } else
            if (info.progress) {
                deferred.notify(info.progress);
            } else {
                stopKeepAlive();
                deferred.resolve(info.response);
            }
        }
        catch (err) {
            var wrappingError = new CFError(
                CFError.errorTypes.Error,
                err,
                "queue '" + self.name + "'' failed to parse response '" + response + "' as json");
            deferred.reject(wrappingError);
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
            var d = domain.create();

            d.on('error', function(err) {
                self.nats.publish(replyTo,
                    "{ \r\n" +
                    "   \"status\": \"error\",\r\n" +
                    "   \"error\": " + jsonifyError(err) + "\r\n" +
                    "}");

                subscribe(workerId);
            });

            d.add(request);
            d.add(replyTo);

            d.run(function() {
                // notify client that we got his message
                self.nats.publish(replyTo, JSON.stringify({
                    status: 'received'
                }));

                // we create keep alive mechanism between the worker and the client
                var kid = setInterval(function() {
                    self.nats.publish(replyTo, JSON.stringify({
                        status: 'keep-alive'
                    }));
                }, self.timeout);

                var data = JSON.parse(request);

                var job = {
                    request:data,
                    progress: function(progressInfo) {
                        self.nats.publish(replyTo, JSON.stringify({
                            progress: progressInfo
                        }));
                    }
                };

                self.nats.publish(replyTo, JSON.stringify({
                    status: 'started'
                }));

                handler(job, function(err, response) {
                    // subscribe again to handle another job
                    subscribe(workerId);

                    if (kid) {
                        clearInterval(kid);
                    }

                    if (err) {
                        self.nats.publish(replyTo,
//nathang: can't JSON.stringify(error) - returns an empty object, because not enumerable
                            "{ \r\n" +
                            "   \"status\": \"error\",\r\n" +
                            "   \"error\": " + jsonifyError(err) + "\r\n" +
                            "}");
                        return;
                    }

                    self.nats.publish(replyTo, JSON.stringify({
                        status: 'finished',
                        response: response
                    }));
                });
            });

        });
    };

    var totalWorkers = opts.workers || 1;
    for (var pos=0; pos<totalWorkers; pos++) {
        subscribe(pos);
    }
};
