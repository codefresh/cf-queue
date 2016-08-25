'use strict';

var proxyquire = require('proxyquire').noCallThru();
var Q          = require('q');
var chai       = require('chai');
var expect     = chai.expect;
var sinon      = require('sinon');
var sinonChai  = require('sinon-chai');
chai.use(sinonChai);
var domain = require('domain');


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
                        expect(err.toString()).to.equal("QueueError: queue: 'myChannel' failed to parse response: '{\"status\":\"finished\",\"response\":\"response\"}' as json; caused by TypeError: must start with number, buffer, array or string");
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
                        expect(job.request).to.deep.equal({ field: 'value' });
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
                        expect(job.request).to.deep.equal({ field: 'value' });
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
                        expect(job.request).to.deep.equal({ field: 'value' });
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
                        expect(job.request).to.deep.equal({ field: 'value' });
                        expect(unsubscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        expect(subscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        callback();
                        expect(subscribeSpy).to.have.been.calledTwice; // jshint ignore:line
                        done();
                    });
                });
                queue.process(handler);
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
                        expect(job.request).to.deep.equal({ field: 'value' });
                        expect(subscribeSpy).to.have.been.calledOnce; // jshint ignore:line
                        callback(new Error("handling error"));
                    });
                });
                queue.process(handler);
            });

        });

    });
});

