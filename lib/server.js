var DEFAULT_PORT = 8080;

var http = require('http');
var ws = require('ws');
var redis = require('redis');

var handleConnection = require('./handleConnection');
var db = require('./db');
var dashboard = require('./dashboard');

var port = process.env.PORT || DEFAULT_PORT;
var connections = {};
var server = http.createServer(dashboard.getResponderMiddleware(connections));

var wss = new ws.Server({ server: server });

var rclient = redis.createClient(/* TODO: options*/);

rclient.on('error', function (err) {
  console.error('Check Redis! Err>', err);
  process.exit(1);
});

db.init(rclient);
wss.on('connection', function connection (wsocket) {
  handleConnection(wsocket, db, connections);
});

server.listen(port);
console.log('Web Socket server listening on port', port);
