"use strict";
var Q     = require('q');
var Queue = require('../lib/queue');

process.on('uncaughtException', function (err) {
    console.error('Uncaught Exception:' + err.stack);
});

var queue = new Queue("myChannel", {
    workers: 50,
    servers: ['nats://192.168.99.100:4222'],
    timeout: 5000
});

var req = {
    field: "myField"
};


var send = () => {
    return Q.delay(1000)
        .then(() => {
            queue.request(req)
                .then(function (res) { // jshint ignore:line
                    console.log(`finished: ${JSON.stringify(res)}`);
                })
                .progress(function (info) { // jshint ignore:line
                    console.log(`progress: ${info.toString()}`);
                })
                .catch(function (err) { // jshint ignore:line
                    console.error(`error: ${JSON.stringify(err.toString())}`);
                });
        })
        .then(() => {
            send();
        });
};

send();


