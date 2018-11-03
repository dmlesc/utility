'use strict';
process.on('uncaughtException', (err) => { console.log(err); });

const fs = require('fs');
const zlib = require('zlib');
const elasticsearch = require('elasticsearch');

const elastic = new elasticsearch.Client({
  host: 'http://blak-es0:9200'
  //, log: 'trace'
});

const src = process.argv[2];
const dst = process.argv[3];
const index_name = process.argv[4];

var logs = [];
var howmany = 5000;
var files_processed = 0;
var currentFile;

var files = fs.readdirSync(src);
console.log('#files:', files.length);

function startProcess(file) { 
  loadFile(file);
}

function loadFile(file) {
  currentFile = src + file;
  logs = JSON.parse(zlib.gunzipSync(fs.readFileSync(currentFile)));
  files_processed++;
  console.log(currentFile, files_processed, files.length);

  parseLogs(logs.splice(0, howmany));
}

function parseLogs(batch) {
  var bulk = [];
  var action = {};
  action.index = { _index: index_name, _type: 'doc'};

  for (var i=0; i < batch.length; i++) {
    var doc = batch[i];
    bulk.push(action);
    bulk.push(doc);
  }

  elasticBulk(bulk);
}

function elasticBulk(bulk) {
  elastic.bulk({ body: bulk }, (err, res) => {
    if (err) { console.log(err); }
    else {
      //console.log(res);
      console.log('logs.length:', logs.length, '    ');

      if (logs.length > 0) {
        parseLogs(logs.splice(0, howmany));
      }
      else {
        //console.log('finished processing: ' + currentFile);
        cleanUp();
      }
    }
  });
}

function cleanUp() {
  fs.renameSync(currentFile, currentFile.replace(src, dst));
  console.log('cleaned up: ' + currentFile);

  if (files.length) {
    startProcess(files.shift());
  }
  else {
    console.log('no more files to process');
    console.log('files_processed:', files_processed);
  }
}

startProcess(files.shift());