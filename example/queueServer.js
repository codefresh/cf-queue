"use strict";

var Queue = require('../lib/queue');

var queue = new Queue("myChannel", {
    workers: 50,
    servers: ['nats://192.168.99.100:4222']
});



queue.process(function(request, callback){
    request.progress("progress");
    setTimeout(() => {
        console.log(`got request: ${JSON.stringify(request)}`);
        callback(null, "ok");
        //callback(null, "ok");
        request.progress("progress");
    }, 4000);
});