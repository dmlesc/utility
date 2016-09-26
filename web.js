'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');

var sitePath = './callnotes';
var ct = [];
ct['.html'] = 'text/html';
ct['.css'] = 'text/css';
ct['.js'] = 'application/javascript';
ct['.json'] = 'application/json';

var server = http.createServer((req, res) => {
  console.log(req.url);
  var url = req.url;
  var contenttype, code;
  var file = sitePath;
  if (url === '/')
    file += '/index.html';
  else
    file += url;
  fs.readFile(file, (err, data) => {
    if (err)
      sendResponse(404, 'text/plain', 'not found', res);
    else {
      contenttype = ct[path.extname(file)];
      if (!contenttype)
        contenttype = 'text/plain';
      sendResponse(200, contenttype, data, res);
    }
  });
});

function sendResponse(code, contenttype, data, res) {
  res.writeHead(200, { 'Content-Type': contenttype });
  res.end(data);
}
server.listen(80, '192.168.2.12');
console.log('webserver started');