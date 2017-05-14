'use strict';

var proxyquire = require('proxyquire').noCallThru();
var Q          = require('q');
var chai       = require('chai');
var expect     = chai.expect;
var sinon      = require('sinon');
var sinonChai  = require('sinon-chai');
chai.use(sinonChai);
var domain = require('domain');

describe('constructor', function(){

    var Queue, connectSpy, readFileSyncSpy, createSecureContextSpy;
    beforeEach(function(){
        connectSpy = sinon.spy();
        readFileSyncSpy = sinon.spy();
        createSecureContextSpy = sinon.spy();
        Queue          = proxyquire('../lib/queue', {
            'nats': {
                connect: connectSpy
            },
            'fs': {
                readFileSync: readFileSyncSpy
            },
            'tls': {
                createSecureContext: createSecureContextSpy
            }
        });
    });

    it('without Tls', function(){
      var opts = {
          servers: ['srv1:4222', 'srv2:4222']
      };
      var queue = new Queue('test', opts);
      expect(queue.tls).to.be.false; //jshint ignore:line
      expect(queue.totalWorkers).to.be.equal(50);
      expect(queue.timeout).to.be.equal(60000);

      expect(readFileSyncSpy).to.have.not.been.called; //jshint ignore:line
      expect(createSecureContextSpy).to.have.not.been.called; //jshint ignore:line

    });

    it('with Tls', function(){
        var opts = {
            servers: ['srv1:4222', 'srv2:4222'],
            tls: {
                key: 'key.pem',
                cert: 'cert.pem'
            },
            workers: 100,
            timeout: 120000
        };
        var queue = new Queue('test', opts);

        expect(queue.totalWorkers).to.be.equal(100);
        expect(queue.timeout).to.be.equal(120000);

        expect(queue.tls).to.be.not.empty; //jshint ignore:line

        expect(readFileSyncSpy).to.have.been.calledTwice; //jshint ignore:line
        expect(createSecureContextSpy).to.have.been.calledOnce; //jshint ignore:line

        var natsOpts = connectSpy.firstCall.args[0];
        expect(natsOpts).to.include.keys('tls');
        expect(natsOpts.tls).to.include.keys('cert', 'secureContext');
    });
});

describe('request/process tests', function () {

    describe('request tests', function () {

        describe('positive tests', () => {

            it('should respond with response', () => {
                var unsubscribeSpy = sinon.spy();
                var requestSpy     = sinon.spy((name, payload, callback) => {
                    expect(JSON.parse(new Buffer(payload, 'base64').toString('utf8'))).to.deep.equal({data: {}});
                    process.nextTick(() => {
                        callback(Buffer(JSON.stringify({
                            status: 'finished',
                            response: 'response'
                        }), 'utf8').toString('base64'));
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                request: requestSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {});
                var data           = {};
                return queue.request(data)
                    .then((res) => {
                        expect(res).to.equal("response");
                    });

            });

            it('should add requestId if present on the domain', () => {
                var unsubscribeSpy = sinon.spy();
                var requestSpy     = sinon.spy((name, payload, callback) => {
                    expect(JSON.parse(new Buffer(payload, 'base64').toString('utf8'))).to.deep.equal({
                        requestId: 'requestId',
                        data: {field: "value"}
                    });
                    process.nextTick(() => {
                        callback(Buffer(JSON.stringify({
                            status: 'finished',
                            response: 'response'
                        }), 'utf8').toString('base64'));
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                request: requestSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {});
                var data           = {field: "value"};
                var d              = domain.create();
                d.run(() => {
                    domain.active.requestId = "requestId";
                    return queue.request(data)
                        .then((res) => {
                            expect(res).to.equal("response");
                        });
                });

            });

            it('should notify and not finish in case passed message is with status progress', () => {
                var unsubscribeSpy  = sinon.spy();
                var requestSpy      = sinon.spy((name, payload, callback) => {
                    process.nextTick(() => {
                        callback(Buffer(JSON.stringify({
                            status: 'progress',
                            progress: 'progress message'
                        }), 'utf8').toString('base64'));

                        setTimeout(() => {
                            callback(Buffer(JSON.stringify({
                                status: 'finished',
                                response: 'ok'
                            }), 'utf8').toString('base64'));
                        }, 5);
                    });
                    return 1;
                });
                var Queue           = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                request: requestSpy
                            };
                        }
                    }
                });
                var queue           = new Queue("myChannel", {});
                var data            = {};
                var reachedProgress = false;
                return queue.request(data)
                    .progress((res) => {
                        expect(res).to.equal("progress message");
                        reachedProgress = true;
                        expect(unsubscribeSpy).to.not.have.been.called; // jshint ignore:line
                    })
                    .then((res) => {
                        expect(res).to.equal("ok");
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        if (!reachedProgress) {
                            return Q.reject(new Error("progress message was not called"));
                        }
                    });

            });

            it('should not do anything in case passed message is not a recognized status', (done) => {
                var unsubscribeSpy = sinon.spy();
                var requestSpy     = sinon.spy((name, payload, callback) => {
                    process.nextTick(() => {
                        callback(Buffer(JSON.stringify({
                            status: 'no-existing-status',
                            progress: 'progress message'
                        }), 'utf8').toString('base64'));
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                request: requestSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {});
                var data           = {};
                queue.request(data)
                    .done(() => {
                        return Q.reject(new Error("should have not reached here"));
                    }, done);

                setTimeout(() => {
                    expect(unsubscribeSpy).to.not.have.been.called; // jshint ignore:line
                    done();
                }, 10);

            });

            it('should not do anything in case passed message does not have status field', (done) => {
                var unsubscribeSpy = sinon.spy();
                var requestSpy     = sinon.spy((name, payload, callback) => {
                    process.nextTick(() => {
                        callback(Buffer(JSON.stringify({}), 'utf8').toString('base64'));
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                request: requestSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {});
                var data           = {};
                queue.request(data)
                    .done(() => {
                        return Q.reject(new Error("should have not reached here"));
                    }, done);

                setTimeout(() => {
                    expect(unsubscribeSpy).to.not.have.been.called; // jshint ignore:line
                    done();
                }, 10);

            });


        });

        describe('negative tests', () => {

            it('should return an error in case convertingFromRequest throws an error', () => {
                var unsubscribeSpy = sinon.spy();
                var requestSpy     = sinon.spy((name, payload, callback) => {
                    process.nextTick(() => {
                        callback({
                            status: 'finished',
                            response: 'response'
                        });
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                request: requestSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {});
                var data           = {};
                return queue.request(data)
                    .then(() => {
                        return Q.reject(new Error("should have failed"));
                    }, (err) => {
                        expect(err.toString()).to.contain("QueueError: queue: 'myChannel' failed to parse response: '{\"status\":\"finished\",\"response\":\"response\"}' as json; caused by TypeError:");
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                    });

            });

            it('should return a timeout error in case no response or error was received', () => {
                var unsubscribeSpy = sinon.spy();
                var requestSpy     = sinon.spy(() => {
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                request: requestSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {timeout: 1});
                var data           = {};
                return queue.request(data)
                    .then(() => {
                        return Q.reject(new Error("should have failed"));
                    }, (err) => {
                        expect(err.toString()).to.equal("QueueTimeoutError: Timeout for queue request to channel: myChannel");
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                    });
            });

            it('should not unsubscribe more than once if a response was received after timeout', (done) => {
                var unsubscribeSpy = sinon.spy();
                var requestSpy     = sinon.spy((name, payload, callback) => {
                    setTimeout(() => {
                        callback({
                            status: 'finished',
                            response: 'response'
                        });
                    }, 10);
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                request: requestSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {timeout: 1});
                var data           = {};
                queue.request(data)
                    .then(() => {
                        return Q.reject(new Error("should have failed"));
                    }, (err) => {
                        expect(err.toString()).to.equal("QueueTimeoutError: Timeout for queue request to channel: myChannel");
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        return Q.delay(20);
                    })
                    .then(() => {
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                    })
                    .done(done, done);
            });

            it('should reject with an error in case passed message is with status error', () => {
                var unsubscribeSpy = sinon.spy();
                var requestSpy     = sinon.spy((name, payload, callback) => {
                    process.nextTick(() => {
                        callback(Buffer(JSON.stringify({
                            status: 'error',
                            error: 'error message'
                        }), 'utf8').toString('base64'));
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                request: requestSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {});
                var data           = {};
                return queue.request(data)
                    .then(() => {
                        return Q.reject(new Error("should have failed"));
                    }, (err) => {
                        expect(err.toString()).to.equal("error message");
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                    });

            });

        });

    });

    describe('process tests', () => {

        describe('positive tests', () => {

            it('should handle request successfully', (done) => {
                var subscribed     = false;
                var unsubscribeSpy = sinon.spy();
                var publishSpy     = sinon.spy();
                var subscribeSpy   = sinon.spy((name, options, callback) => {
                    process.nextTick(() => {
                        if (!subscribed) {
                            subscribed = true;
                            callback(Buffer(JSON.stringify({
                                data: {
                                    field: "value"
                                }
                            }), 'utf8').toString('base64'));
                        }
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                subscribe: subscribeSpy,
                                publish: publishSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {workers: 2});
                var handler        = sinon.spy((job, callback) => {
                    var d = domain.create();
                    d.on('error', (err) => {
                        done(err);
                    });
                    d.run(() => {
                        expect(job.request).to.deep.equal({field: 'value'});
                        callback();
                        expect(publishSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(unsubscribeSpy).to.not.have.been.called; // jshint ignore:line
                        callback();
                        done();
                    });
                });
                queue.process(handler);
            });

            it('should not run the code that runs after the callback was fired more than once if for some reason it was called more than once', (done) => {
                var subscribed     = false;
                var unsubscribeSpy = sinon.spy();
                var publishSpy     = sinon.spy();
                var subscribeSpy   = sinon.spy((name, options, callback) => {
                    process.nextTick(() => {
                        if (!subscribed) {
                            subscribed = true;
                            callback(Buffer(JSON.stringify({
                                data: {
                                    field: "value"
                                }
                            }), 'utf8').toString('base64'));
                        }
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                subscribe: subscribeSpy,
                                publish: publishSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {workers: 2});
                var handler        = sinon.spy((job, callback) => {
                    var d = domain.create();
                    d.on('error', (err) => {
                        done(err);
                    });
                    d.run(() => {
                        expect(job.request).to.deep.equal({field: 'value'});
                        callback();
                        expect(publishSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(unsubscribeSpy).to.not.have.been.called; // jshint ignore:line
                        callback();
                        expect(publishSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(unsubscribeSpy).to.not.have.been.called; // jshint ignore:line
                        done();
                    });
                });
                queue.process(handler);
            });

            it('should unsubscribe if reached limit of workers', (done) => {
                var subscribed     = false;
                var unsubscribeSpy = sinon.spy();
                var publishSpy     = sinon.spy();
                var subscribeSpy   = sinon.spy((name, options, callback) => {
                    process.nextTick(() => {
                        if (!subscribed) {
                            subscribed = true;
                            callback(Buffer(JSON.stringify({
                                data: {
                                    field: "value"
                                }
                            }), 'utf8').toString('base64'));
                        }
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                subscribe: subscribeSpy,
                                publish: publishSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {workers: 1});
                var handler        = sinon.spy((job) => {
                    var d = domain.create();
                    d.on('error', (err) => {
                        done(err);
                    });
                    d.run(() => {
                        expect(job.request).to.deep.equal({field: 'value'});
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(subscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        done();
                    });
                });
                queue.process(handler);
            });

            it('should subscribe again if reached limit of workers and then one was released', (done) => {
                var subscribed     = false;
                var unsubscribeSpy = sinon.spy();
                var publishSpy     = sinon.spy();
                var subscribeSpy   = sinon.spy((name, options, callback) => {
                    process.nextTick(() => {
                        if (!subscribed) {
                            subscribed = true;
                            callback(Buffer(JSON.stringify({
                                data: {
                                    field: "value"
                                }
                            }), 'utf8').toString('base64'));
                        }
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                subscribe: subscribeSpy,
                                publish: publishSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {workers: 1});
                var handler        = sinon.spy((job, callback) => {
                    var d = domain.create();
                    d.on('error', (err) => {
                        done(err);
                    });
                    d.run(() => {
                        expect(job.request).to.deep.equal({field: 'value'});
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(subscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        callback();
                        expect(subscribeSpy).to.have.been.calledTwice; // jshint ignore:line
                        done();
                    });
                });
                queue.process(handler);
            });

            it('should call connection unsubscribe in case of unsubscribing', (done) => {
                var subscribed     = false;
                var unsubscribeSpy = sinon.spy();
                var publishSpy     = sinon.spy();
                var subscribeSpy   = sinon.spy((name, options, callback) => {
                    process.nextTick(() => {
                        if (!subscribed) {
                            subscribed = true;
                            callback(Buffer(JSON.stringify({
                                data: {
                                    field: "value"
                                }
                            }), 'utf8').toString('base64'));
                        }
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                subscribe: subscribeSpy,
                                publish: publishSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {workers: 2});
                var handler        = sinon.spy((job, callback) => {
                    var d = domain.create();
                    d.on('error', (err) => {
                        done(err);
                    });
                    d.run(() => {
                        expect(job.request).to.deep.equal({field: 'value'});
                        callback();
                        expect(publishSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(unsubscribeSpy).to.not.have.been.called; // jshint ignore:line
                        callback();
                        unsubscribe();
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        done();
                    });
                });
                var unsubscribe = queue.process(handler); // jshint ignore:line
            });

            it('should not succeed to subscribe again in case unsubscribe was called and after that a handler of a previous request finishes its execution and tries to re-subscribe', (done) => {
                var subscribed     = false;
                var unsubscribeSpy = sinon.spy();
                var publishSpy     = sinon.spy();
                var subscribeSpy   = sinon.spy((name, options, callback) => {
                    setTimeout(() => {
                        if (!subscribed) {
                            subscribed = true;
                            callback(Buffer(JSON.stringify({
                                data: {
                                    field: "value"
                                }
                            }), 'utf8').toString('base64'));
                        }
                    }, 500);
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                subscribe: subscribeSpy,
                                publish: publishSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {workers: 2});
                var handler        = sinon.spy((job, callback) => {
                    var d = domain.create();
                    d.on('error', (err) => {
                        done(err);
                    });
                    d.run(() => {
                        expect(job.request).to.deep.equal({field: 'value'});
                        callback();
                        expect(publishSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(subscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        callback();
                        done();
                    });
                });
                var unsubscribe = queue.process(handler);
                unsubscribe();
            });

            it('should subscribe again in case worker reached max, completed and only after that unsubscribe was called', (done) => {
                var subscribed     = false;
                var unsubscribeSpy = sinon.spy();
                var publishSpy     = sinon.spy();
                var subscribeSpy   = sinon.spy((name, options, callback) => {
                    setTimeout(() => {
                        if (!subscribed) {
                            subscribed = true;
                            callback(Buffer(JSON.stringify({
                                data: {
                                    field: "value"
                                }
                            }), 'utf8').toString('base64'));
                        }
                    }, 500);
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                subscribe: subscribeSpy,
                                publish: publishSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {workers: 1});
                var handler        = sinon.spy((job, callback) => {
                    var d = domain.create();
                    d.on('error', (err) => {
                        done(err);
                    });
                    d.run(() => {
                        expect(job.request).to.deep.equal({field: 'value'});
                        callback();
                        expect(publishSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(subscribeSpy).to.have.been.calledTwice; // jshint ignore:line
                        unsubscribe();
                        expect(unsubscribeSpy).to.have.been.calledTwice; // jshint ignore:line
                        callback();
                        done();
                    });
                });
                var unsubscribe = queue.process(handler); // jshint ignore:line
            });

        });

        describe('negative tests', () => {

            it('should return an error in case callback was fired with an error', (done) => {
                var subscribed     = false;
                var unsubscribeSpy = sinon.spy();
                var publishSpy     = sinon.spy((replyTo, data) => {
                    data = JSON.parse(new Buffer(data, 'base64').toString('utf8'));
                    expect(data).to.deep.equal({
                        "error": "Error: handling error",
                        "status": "error"
                    });
                    done();
                });
                var subscribeSpy   = sinon.spy((name, options, callback) => {
                    process.nextTick(() => {
                        if (!subscribed) {
                            subscribed = true;
                            callback(Buffer(JSON.stringify({
                                data: {
                                    field: "value"
                                }
                            }), 'utf8').toString('base64'));
                        }
                    });
                    return 1;
                });
                var Queue          = proxyquire('../lib/queue', {
                    'nats': {
                        connect: () => {
                            return {
                                unsubscribe: unsubscribeSpy,
                                subscribe: subscribeSpy,
                                publish: publishSpy
                            };
                        }
                    }
                });
                var queue          = new Queue("myChannel", {workers: 2});
                var handler        = sinon.spy((job, callback) => {
                    var d = domain.create();
                    d.on('error', (err) => {
                        done(err);
                    });
                    d.run(() => {
                        expect(job.request).to.deep.equal({field: 'value'});
                        expect(subscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        callback(new Error("handling error"));
                    });
                });
                queue.process(handler);
            });

        });

    });
});

