var Q = require('q'),
    domain = require('domain'),
    CFError = require('cf-errors');

var convertToRequest = function(data) {
    var payload = data;
    if ('object' === typeof data) {
        var str = JSON.stringify(data);
        payload = new Buffer(str, 'utf8').toString('base64');
    }
    return payload;
};

var convertFromRequest = function(response) {
    var str = new Buffer(response, 'base64').toString('utf8');
    try {
        return JSON.parse(str);
    } catch (ex) {
        return str;
    }
};

var Queue = module.exports = function(name, opts) {
    this.name = name || opts.name;
    this.totalWorkers = opts.workers || 1;
    this.runningWorkers = 0;

    // default timeout of 30sec for someone to start handling the request
    this.timeout = opts.timeout || 10*1000;

    this.nats = require('./nats').connect({
        url: opts.servers || 'nats://localhost:4222',
        reconnect: true,
        verbose: true,
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

    var payload = convertToRequest(data);

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

    var sid = this.nats.request(this.name, payload, function(response) {
        try {
            // on any response from the worker we reset the keep alive
            keepAlive();

            // if this request already timed out, we don't want to handle it
            if (timedout) {
                return;
            }

            var info = convertFromRequest(response);

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
                self.nats.unsubscribe(sid);
                stopKeepAlive();
                deferred.reject(new Error(info.error));
            } else
            if (info.progress) {
                deferred.notify(info.progress);
            } else {
                self.nats.unsubscribe(sid);
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

    var subscribed = false;

    var subscribe = function() {
        if (subscribed) {
            console.log('subscriber already waiting, return');
            return;
        }
        subscribed = true;
        self.runningWorkers++;

        console.log('subscribing: total[' + self.totalWorkers + '], running[' + + self.runningWorkers + ']');

        self.nats.subscribe(self.name, {queue:self.name, max: 1}, function(request, replyTo) {

            subscribed = false;

            console.log('got job: total[' + self.totalWorkers + '], running[' + + self.runningWorkers + ']');

            if (self.runningWorkers < self.totalWorkers) {
                subscribe();
            } else {
                console.log('max workers reach: total[' + self.totalWorkers + '], running[' + + self.runningWorkers + ']');
            }

            var d = domain.create();

            d.on('error', function(err) {
                console.error('>>>>> ERROR while handling request ' + self.name + ' with args ' + JSON.stringify(request));
                console.error(err.stack);

                var error = {
                    status: "error",
                    error: err.toString()
                };

                self.nats.publish(replyTo, convertToRequest(error));

                self.runningWorkers--;
                console.log('job finished with error: total[' + self.totalWorkers + '], running[' + + self.runningWorkers + ']');
                subscribe();
            });

            d.add(request);
            d.add(replyTo);

            d.run(function() {
                // notify client that we got his message
                self.nats.publish(replyTo, convertToRequest({
                    status: 'received'
                }));

                // we create keep alive mechanism between the worker and the client
                var kid = setInterval(function() {
                    self.nats.publish(replyTo, convertToRequest({
                        status: 'keep-alive'
                    }));
                }, self.timeout);

                var data = convertFromRequest(request);

                var job = {
                    request:data,
                    progress: function(progressInfo) {
                        self.nats.publish(replyTo, convertToRequest({
                            progress: progressInfo
                        }));
                    }
                };

                self.nats.publish(replyTo, convertToRequest({
                    status: 'started'
                }));

                handler(job, function(err, response) {

                    // subscribe again to handle another job
                    self.runningWorkers--;
                    console.log('job finished: total[' + self.totalWorkers + '], running[' + + self.runningWorkers + ']');
                    subscribe();

                    if (kid) {
                        clearInterval(kid);
                    }

                    if (err) {
                        console.error('>>>>> ERROR');
                        console.error(err);

                        var error = {
                            status: "error",
                            error: err.toString()
                        };

                        return self.nats.publish(replyTo, convertToRequest(error));
                    }

                    self.nats.publish(replyTo, convertToRequest({
                        status: 'finished',
                        response: response
                    }));
                });
            });

        });
    };

    subscribe();
};
