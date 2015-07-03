var debug = require('debug')('wstq:connhandler');

var uuid = require('uuid');

module.exports = handleConnection;

var STATUS_AWAITING_CLIENT_ID = 'STATUS_AWAITING_CLIENT_ID';
var STATUS_AWAITING_TASK_COMPLETION = 'STATUS_AWAITING_TASK_COMPLETION';
var STATUS_AWAITING_NEW_TASK = 'STATUS_AWAITING_NEW_TASK';
var STATUS_WSOCKET_CLOSED = 'STATUS_WSOCKET_CLOSED';

var MSGTYPE_HANDSHAKE = 'handshake';
var MSGTYPE_ACK = 'ack';
var MSGTYPE_BUSY = 'busy';

var WS_CLOSE_CODE_SERVER_FAIL = 1011;

var clientCount = 0;

function createConnection (wsocket) {
  var newConnection = {
    wsocket: wsocket,
    tempId: uuid.v4(),
    status: STATUS_AWAITING_CLIENT_ID,
    clientId: null,
    disableTaskEmitter: null
  };
  return newConnection;
}

function handleConnection (wsocket, db, connections) {
  clientCount++;
  console.log('New websocket client connected (total: ' + clientCount + ')');

  var conn = createConnection(wsocket);
  connections[conn.tempId] = conn;

  var handler = createHandler(conn, db, connections);

  handler();
}

function createHandler (conn, db, connections) {
  return function () {
    conn.wsocket.on('message', function incoming (messageString) {
      debug('incoming message "%s"', messageString);
      try {
        var message = JSON.parse(messageString);
      } catch (e) {
        return console.error(
          'Error: incoming message is not a valid JSON object'
        );
      }

      function sendTask (task) {
        debug('sending task to client "%s"', conn.clientId);
        var taskMessage = {
          type: 'task',
          payload: task
        };
        conn.wsocket.send(JSON.stringify(taskMessage));
        conn.status = STATUS_AWAITING_TASK_COMPLETION;
        conn.currentTask = task;
      }

      function closeOnError (err) {
        debug('closing socket on error for client "%s"', conn.clientId);
        debug('error: ', err.message);
        conn.wsocket.close(WS_CLOSE_CODE_SERVER_FAIL, err.message);
      }

      function onTask (err, task) {
        debug('receiving task for "%s"', conn.clientId);
        if (err) { return closeOnError(err); }
        sendTask(task);
      }

      switch (message.type) {
        // Handshake
        case MSGTYPE_HANDSHAKE:
          debug('received handshake from "%s"', conn.tempId);
          if (conn.status === STATUS_AWAITING_CLIENT_ID) {

            if (connections[message.clientId]) {
              return closeOnError(
                new Error("Duplicate ClientId connection.")
              );
            }

            delete connections[conn.tempId];
            connections[message.clientId] = conn;
            conn.clientId = message.clientId;

            db.getPendingTask(
              conn.clientId,
              function gotpending (err, task) {
                if (err) { return closeOnError(err); }
                if (task) {
                  debug('reconnect detected for "%s"', conn.clientId);
                  sendTask(task);
                } else {
                  debug('waiting for tasks for "%s"', conn.clientId);
                  conn.status = STATUS_AWAITING_NEW_TASK;
                  conn.disableTaskEmitter =
                      db.onTaskFor(conn.clientId, onTask);
                }
              }
            );
          } else {
            console.warn('Protocol violation: redundand handshake.');
          }
          return;

        // Task acknowledge
        case MSGTYPE_ACK:
          debug('acknowledge received from "%s"', conn.clientId);
          if (conn.status === STATUS_AWAITING_TASK_COMPLETION) {
            db.ackTask(
              conn.clientId,
              message.taskId,
              message.payload,
              function onacktask (err) {
                if (err) { return closeOnError(err); }
                debug('ack success,on to new tasks for "%s"', conn.clientId);
                conn.status = STATUS_AWAITING_NEW_TASK;
                conn.disableTaskEmitter =
                    db.onTaskFor(conn.clientId, onTask);
              }
            );
          } else {
            console.warning(
              'Protocol violation: received task ack when in "' +
                conn.status + '" status.'
            );
          }
          return;

        // Client busy
        case MSGTYPE_BUSY:
          debug('received busy from "%s"', conn.clientId);
          if (conn.status === STATUS_AWAITING_TASK_COMPLETION) {
            console.log('Client already busy "' + conn.clientId + '"');
          } else {
            console.warn(
              'Protocol violation: received "client busy" when in "' +
                  conn.status + '" status.'
            );
          }
          return;

        // Unknown message type
        default:
          debug('received unknown task type from "%s"', conn.clientId);
          console.error(
            'Unknow incoming message type "' +
                (message.type || '<emtpy>') + '"'
          );
          return;
      }
    });

    conn.wsocket.on('error', function onerror (error) {
      debug('WS error for "%s"', conn.clientId);
      console.error('WS Error for', conn.clientId, error);
    });

    conn.wsocket.on('close', function onclose (code, msg) {
      debug('close socket event for "%s"', conn.clientId);
      clientCount--;
      console.log(
        'Client disconnected with code "'  + code +
            '" and message "' + msg + '" (total: ' + clientCount + ')'
      );
      if (conn.status === STATUS_AWAITING_TASK_COMPLETION) {
        conn.disableTaskEmitter();
        console.log('...leaving unfinished task.');
      }
      conn.status = STATUS_WSOCKET_CLOSED;
      delete connections[conn.clientId || conn.tempId];
    });
  }
}
