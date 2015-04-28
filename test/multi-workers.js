var assert = require('assert');
var Queue = require('../index');


var Worker = function(name) {

    var queue = new Queue('test-queue', {});
    var working = false;

    console.log('Stated worker ' + name);

    queue.process({workers:10}, function(job, callback) {
        if (working) {
            console.log('worker ' + name + ' >>> already working');
            return callback('Already working');
        }
        working = true;

        console.log('worker ' + name + ' >>> got request');

        var progress = function(step) {
            return function() {
                job.progress({worker:name, step:step});
            }
        };

        setTimeout(progress('step 1'), 500);
        setTimeout(progress('step 2'), 1000);
        setTimeout(progress('step 3'), 1500);
        setTimeout(progress('step 4'), 2000);
        setTimeout(function() {
            working = false;
            callback(null, job.request);
        }, 2500);

    });
}

var worker1 = new Worker('worker1');
//var worker2 = new Worker('worker2');
//var worker3 = new Worker('worker3');
//var worker4 = new Worker('worker4');
//var worker4 = new Worker('worker4');
//var worker5 = new Worker('worker5');

var queue = new Queue('test-queue', {});
var count = 1;
setInterval(function() {
    var id = count++;
    var data = {id:id};

    console.log('server >>> sending request ' + JSON.stringify(data));
    queue.request(data)
        .then(function(response) {
            console.log('server >>> got response >>> ' + JSON.stringify(response));
        })
        .progress(function(info) {
            console.log('server >>> got progress >>> ' + JSON.stringify(info));
        })
        .catch(function(err) {
            console.error('server >>> got error >>> ' + err);
        })
}, 1000);

/*
describe ('Multi Workers', function() {

    var queue;

    before(function(done) {

        done();
    });

    after(function(done) {
        done();
    });


    it ('Start Worker1', function(done) {
        queue.process(function(job, callback) {
        });
        done();

    });

    it ('Start Worker2', function(done) {
        queue.process(function(job, callback) {
        });
        done();
    });

});
    */