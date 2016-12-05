'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');

var sitePath = './' + process.argv[2];
var ct = [];
ct['.html'] = 'text/html';
ct['.css'] = 'text/css';
ct['.js'] = 'application/javascript';
ct['.json'] = 'application/json';

var server = http.createServer((req, res) => {
  console.log(new Date().toJSON() + ' - ' + req.url);
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
  res.writeHead(code, { 'Content-Type': contenttype });
  res.end(data);
}
server.listen(12345, '10.10.0.221');
console.log('ready to serve');