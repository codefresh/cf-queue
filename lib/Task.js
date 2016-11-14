"use strict";

var kue = require('kue');
var express = require('express');
var ui = require('kue-ui');
var app = express();



var queue = kue.createQueue({
    redis: {
        host: 'codefresh.dev'
    }
});

ui.setup({
    apiURL: '/api', // IMPORTANT: specify the api url
    baseURL: '/kue', // IMPORTANT: specify the base url
    updateInterval: 5000 // Optional: Fetches new data every 5000 ms
});

// Mount kue JSON api
app.use('/api', kue.app);
// Mount UI
app.use('/kue', ui.app);

app.listen(3000);



var job = queue.create('email', {
    title: 'welcome email for tj'
    , to: 'tj@learnboost.com'
    , template: 'welcome-email'
}).save( function(err){
    if( !err ) console.log( job.id );
});