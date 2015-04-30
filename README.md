# cf-queue - Queue wrapper for nats.io server

## Creating a queue reference

```javascript
var Queue   = require('cf-queue');

var options = {
  servers: [
    'nats://localhost:4222'
  ]
};

var queue = new Queue('queue-name', options);
```

The same initalization code is used by the clien and worker sides.

## How to use it

### Client side

When a client would like to push request into the queue it should use the queue request function and pass the request object.

```javascript
var request = {
  id: 1,
  message: 'hello'
}

queue.request(request)
  .then(function(response) {
    console.log(response);
  })
  .catch(function(err) {
      console.error(err);
  });
```

### Worker side

```javascript
queue.process(function(job, callback) {
  var request = job.request;
  
  var response = {
    id: request.id,
    message: request.message + ' world !!!'
  };
  
  callback(null, response);
}
```

As first parameter for the callback you can return error message or object that will be sent back to the client as error.


## Progress

### Client side

When a client would like to push request into the queue it should use the queue request function and pass the request object.

```javascript
var request = {
  id: 1,
  message: 'hello'
}

queue.request(request)
  .then(function(response) {
    console.log(response);
  })
  .progress(function(message) {
    console.log('PROGRESS >>> ' + message);
  })
  .catch(function(err) {
      console.error(err);
  });
```

### Worker side

```javascript
queue.process(function(job, callback) {
  var request = job.request;
  
  var response = {
    id: request.id,
    message: request.message + ' world !!!'
  };
  
  var counter = 0;
  var sendProgress = function() {
    counter++;
    job.progress('Step ' + counter + ' in ' + request.id);
  
    if (counter === 5) {
      callback(null, response);
    } else {
      setTimeout(sendProgress, 500);
    }
  };
  sendProgress();
}
```

# Docker

You can see example of how to use the queue in Docker in the attached Dockerfile and docker compose file.
The docker compose file load one client, two workers and the queue server.


