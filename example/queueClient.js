"use strict";

var Queue  = require('../lib/queue');

process.on('uncaughtException', function (err)
{
    console.error('Uncaught Exception:' + err.stack);
});

var queue = new Queue("myChannel", {
    workers: 50,
    servers: ['nats://192.168.99.100:4222'],
    timeout: 1000
});

var req = {
    field: "myField"
};


for (var i = 0; i < 1; i++){
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
}
