var db = require('./db');

module.exports.getResponderMiddleware = getResponderMiddleware;

var DOC_TEMPLATE =
  '<!doctype html><html lang="ru"><head><title>WS Queue Server</title>' +
  '<meta charset="UTF-8"></head><body><small>(для обновления перезагрузите ' +
  'страницу)</small><h3>Соединения</h3><p>Всего соединений: ${connCount}' +
  '</p><div>${connections}</div><h3>Текущие задания:</h3><div>${pending}' +
  '</div><body></html>';

var CONNECTION_TEMPLATE =
  '<pre>${connJson}</pre>';

var PENDING_TEMPLATE =
  '<pre>${pendingJson}</pre>';

function renderConnection (connState) {
  var connJson = JSON.stringify({
    status: connState.status,
    clientId: connState.clientId,
    tempId: connState.tempId
  }, null, 4);
  return CONNECTION_TEMPLATE.replace('${connJson}', connJson);
}

function renderPending (allPending) {
  for (key in allPending) {
    allPending[key] = JSON.parse(allPending[key]);
  }
  var pendingJson = JSON.stringify(allPending, null, 4);
  return PENDING_TEMPLATE.replace('${pendingJson}', pendingJson);
}

function renderDocument (connections, allPending) {
  var keys = Object.keys(connections);
  var connPartial = keys.map(
    function (connKey) { return renderConnection(connections[connKey]); }
  ).join('');

  return (
    DOC_TEMPLATE
      .replace('${connCount}', keys.length)
      .replace('${connections}', connPartial)
      .replace('${pending}', renderPending(allPending))
  );
}

function getResponderMiddleware (connections) {
  return function (req, res) {
    db.getAllPending(function (err, allPending) {
      res.writeHead(200, {'Content-Type': 'text/html'});
      res.end(renderDocument(connections, allPending));
    });
  };
}
