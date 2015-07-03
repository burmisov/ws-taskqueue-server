var debug = require('debug')('wstq:db');

var CHECK_INTERVAL = 1 * 1000; // ms

module.exports = {
  init: init,
  getPendingTask: getPendingTask,
  ackTask: ackTask,
  onTaskFor: onTaskFor,
  getAllPending: getAllPending
};

var rclient;
var prefix;

function init (redisClient, keyPrefix) {
  rclient = redisClient;
  prefix = keyPrefix ? (keyPrefix + ':') : '';
  debug('init, prefix="%s"', prefix);
}

var getkey = {
  newTasks: function (clientId) { return prefix + 'tasks-new:' + clientId },
  pending: function () { return prefix + 'tasks-pending' },
  finished: function () { return prefix + 'tasks-finished' }
};

function getPendingTask (clientId, callback) {
  debug('getPendingTask clientId="%s"', clientId);
  rclient.hget(
    getkey.pending(),
    clientId,
    function gotpending (err, taskString) {
      if (err) { return callback(err); }
      debug('getPendingTask clientId="%s" result="%s"', clientId, taskString);
      if (!taskString) { return callback(null, null); }

      try {
        var task = JSON.parse(taskString);
      } catch (e) {
        return callback(new Error('DB task object corrupted:', taskString));
      }

      return callback(null, task);
    }
  );
}

function onTaskFor (clientId, callback) {
  debug('onTaskFor clientId="%s"', clientId);
  var timeoutHandle;

  function checkForTasks () {
    rclient.lpop(getkey.newTasks(clientId), function onpop (err, taskString) {
      if (err) { return callback(err); }
      if (!taskString) {
        timeoutHandle = setTimeout(checkForTasks, CHECK_INTERVAL);
        return;
      }

      try {
        var task = JSON.parse(taskString);
      } catch (e) {
        return callback(e);
      }

      debug('onTaskFor clientId="%s" result="%s"', clientId, taskString);

      rclient.hset(
        getkey.pending(),
        task.ClientId,
        taskString,
        function onhset (err) {
          if (err) { return callback(err); }
          return callback(null, task);
        }
      );
    });
  }

  checkForTasks();

  return function off () {
    debug('Cancelling onTaskFor clientId="%s"', clientId);
    clearTimeout(timeoutHandle);
  };
}

function ackTask (clientId, taskId, result, callback) {
  debug('ackTask clientId="%s" taskId="%s"', clientId, taskId);
  getPendingTask(clientId, function gottask (err, task) {
    if (err) { return callback(err); }

    if (!task) {
      debug('ackTask clientId="%s" taskId="%s" unexistend', clientId, taskId);
      return callback(
        new Error('Tried to acknowledge unexistent task "' + taskId + '"')
      );
    }

    if (task.Id !== taskId) {
      debug('ackTask clientId="%s" taskId="%s" unmatching', clientId, taskId);
      return callback(
        new Error(
          'Tried to acknowledge task with unmatching Id. ' +
          'pending task Id = "' + task.Id + '", ' +
          'ack task Id = "' + taskId + '"'
        )
      );
    }

    task.Result = result;

    rclient.rpush(
      getkey.finished(),
      JSON.stringify(task),
      function onpushtofinished (err) {
        if (err) { return callback(err); }
        rclient.hdel(getkey.pending(), task.ClientId, function onhdel (err) {
          if (err) { return callback(err); }
          debug('ackTask clientId="%s" taskId="%s" OK', clientId, taskId);
          return callback();
        });
      }
    );
  });
}

function getAllPending (callback) {
  rclient.hgetall(getkey.pending(), function gotallpending (err, pending) {
    if (err) { return callback(err); }
    return callback (null, pending);
  });
}
