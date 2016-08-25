'use strict';

var Q       = require('q');
var domain  = require('domain');
var crypto  = require('crypto');
var CFError = require('cf-errors');
var monitor = require('cf-monitor');

var convertToRequest = function (data) {
    var payload = data;
    if ('object' === typeof data) {
        try {
            var str = JSON.stringify(data);
            payload = new Buffer(str, 'utf8').toString('base64');
        }
        catch (err) {
            var error = new CFError({
                name: "QueueError",
                cause: err,
                message: "failed to convert request data to string"
            });
            console.error(error);
            console.dir(data, {depth: 10, colors: true});
            throw error;
        }
    }
    return payload;
};

var convertFromRequest = function (response) {
    var str = new Buffer(response, 'base64').toString('utf8');
    try {
        return JSON.parse(str);
    } catch (ex) {
        return str;
    }
};

var connections = {};

var Queue = module.exports = function (name, opts) {
    this.name            = name || opts.name;
    this.totalWorkers    = opts.workers || 1;
    this.runningWorkers  = 0;
    this.runningHandlers = {};
    this.servers         = opts.servers || 'nats://localhost:4222';

    // default timeout of 30sec for someone to start handling the request
    this.timeout = opts.timeout || 30 * 1000;

    var hash           = crypto.createHash('sha1');
    var connectionHash = hash.update(JSON.stringify(this.servers)).digest('hex');

    var connection = connections[connectionHash];
    if (!connection) {
        connection                  = require('nats').connect({
            servers: this.servers,
            reconnect: true,
            verbose: true,
            maxReconnectAttempts: 60 * 60 * 24, // try to reconnect for 24 hours !!!!
            reconnectTimeWait: 1000
        });
        connections[connectionHash] = connection;
    }

    this.connection = connection;

};

Queue.prototype.request = function (data) {
    var self = this;

    var deferred = Q.defer();
    var timedout = false;
    var finished = false;

    var requestId;
    if (domain.active && domain.active.requestId) {
        requestId = domain.active.requestId;
    }

    var payload = convertToRequest({requestId: requestId, data: data});

    var finishRequest = function () {
        finished = true;
        clearTimeout(tid);
        tid = null;
        self.connection.unsubscribe(sid);
        sid = null;
    };

    var tid = setTimeout(function () { // jshint ignore:line
        timedout  = true;
        var error = new CFError({
            name: "QueueTimeoutError",
            message: `Timeout for queue request to channel: ${self.name}`
        });
        deferred.reject(error);
        finishRequest();
    }, self.timeout);

    var sid = self.connection.request(this.name, payload, function (response) { // jshint ignore:line
        try {
            // if this request already timed out or finished, we don't want to handle it
            if (finished) {
                return;
            }

            var info = convertFromRequest(response);
            if (info.hasOwnProperty('status')) {
                if (info.status === 'error') {
                    finishRequest();
                    deferred.reject(info.error);
                }
                else if (info.status === 'progress') {
                    deferred.notify(info.progress);
                }
                else if (info.status === 'finished') {
                    finishRequest();
                    deferred.resolve(info.response);
                }
            }
        }
        catch (err) {
            finishRequest();
            var wrappingError = new CFError({
                name: "QueueError",
                cause: err,
                message: "queue: '" + self.name + "' failed to parse response: '" + JSON.stringify(response) + "' as json"
            });
            deferred.reject(wrappingError);
        }
    });

    return deferred.promise;
};


Queue.prototype.pause = function () {
    var self = this;

    var keys = Object.keys(self.runningHandlers);
    keys.forEach(function (sid) {
        self.connection.unsubscribe(+(sid));
    });
};

Queue.prototype.unpause = function () {
    var self = this;

    var keys = Object.keys(self.runningHandlers);
    keys.forEach(function (sid) {
        var info = self.runningHandlers[sid];
        self.process(info.opts, info.handler);
        delete self.runningHandlers[sid];
    });
};

Queue.prototype.process = function (opts, handler) {
    var self = this;
    if (!handler) {
        handler = opts;
        opts    = {};
    }

    var subscribed = false;

    var subscribe = function () {
        if (subscribed) {
            console.log(`subscriber for job: ${self.name} is already waiting, returning`);
            return;
        }
        subscribed = true;

        console.log(`subscribing for job: ${self.name}. total[${self.totalWorkers}], running[${self.runningWorkers}]`);

        var sid = self.connection.subscribe(self.name, {queue: self.name}, function (toBeConvertedPayload, replyTo) {

            var payload;
            var d        = domain.create();
            var finished = false;

            var finishProcess = function () {
                finished = true;
                self.runningWorkers--;
                console.log(`job: ${self.name} finished with error. total[${self.totalWorkers}], running[${self.runningWorkers}]`);
                subscribe();
            };

            d.on('error', function (err) {

                var message;
                if (payload) {
                    message = `Uncaught exception during handling of request:${self.name} with args: ${JSON.stringify(payload)}`;
                }
                else {
                    message = `Uncaught exception during handling of request:${self.name} with args ${JSON.stringify(toBeConvertedPayload)}`;
                }
                var error = new CFError({
                    name: "QueueError",
                    cause: err,
                    message: message
                });

                console.error(error.stack);
                monitor.noticeError(error);

                if (!finished) {
                    finishProcess();
                }
            });


            d.run(function () {

                self.runningWorkers++;

                payload = convertFromRequest(toBeConvertedPayload);

                domain.active.requestId = payload.requestId;

                console.log(`got job: ${self.name}. total[${self.totalWorkers}], running[${self.runningWorkers}]`);

                if (self.runningWorkers < self.totalWorkers) {
                } else {
                    console.log(`max workers reached for job: ${self.name}. total[${self.totalWorkers}], running[${self.runningWorkers}]`);

                    self.connection.unsubscribe(sid);
                    delete self.runningHandlers[sid];
                    subscribed = false;
                }

                var job = {
                    request: payload.data,
                    progress: function (progressInfo) {
                        progressInfo = progressInfo || "";
                        self.connection.publish(replyTo, convertToRequest({
                            status: "progress",
                            progress: progressInfo
                        }));
                    }
                };

                handler(job, function (err, response) {

                    if (finished) return;

                    finishProcess();

                    if (err) {
                        var error = {
                            status: "error",
                            error: err.toString()
                        };

                        return self.connection.publish(replyTo, convertToRequest(error));
                    }

                    self.connection.publish(replyTo, convertToRequest({
                        status: 'finished',
                        response: response
                    }));
                });
            });

        });

        self.runningHandlers[sid] = {opts: opts, handler: handler};
    };

    subscribe();
};

Queue.prototype.publish = function (data) {
    var requestId;
    if (domain.active && domain.active.requestId) {
        requestId = domain.active.requestId;
    }

    var payload = convertToRequest({requestId: requestId, data: data});

    this.connection.publish(this.name, payload);
};

Queue.prototype.subscribe = function (handler) {
    var self = this;
    var sid  = self.connection.subscribe(self.name, {}, function (toBeConvertedPayload) {

        var payload;
        var d = domain.create();
        d.on('error', function (err) {
            var message;
            if (payload) {
                message = `Uncaught exception during handling of request:${self.name} with args: ${JSON.stringify(payload)}`;
            }
            else {
                message = `Uncaught exception during handling of request:${self.name} with args ${JSON.stringify(toBeConvertedPayload)}`;
            }
            var error = new CFError({
                name: "QueueError",
                cause: err,
                message: message
            });
            console.error(error.stack);
            monitor.noticeError(error);
        });

        d.run(function () {

            payload = convertFromRequest(toBeConvertedPayload);

            domain.active.requestId = payload.requestId;

            handler(payload.data);
        });

    });

    return sid;
};