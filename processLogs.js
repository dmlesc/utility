'use strict';

const conf = require('./conf/' + process.argv[2]);
const fs = require('fs');
const chokidar = require('chokidar');
const zlib = require('zlib');
const readline = require('readline');
const elasticsearch = require('elasticsearch');
const node_statsd = require('node-statsd');

var elastic = new elasticsearch.Client({
  host: conf.host
  //, log: 'trace'
});

var statsd = new node_statsd({
  host: '10.1.0.116',
  prefix: 'cdnlogs.'
});

//var indexPrefixes = ['cdnlogs-media-', 'cdnlogs-ildccdn-', 'cdnlogs-other-'];
var newPath = 'new';
var inflatedPath = 'inflated';
var processedPath = 'processed';
var logPath = 'logs/info.log';
var errorPath = 'logs/error.log';
var newFiles = {};
var queuedFiles = [];
var interval = 15000;  // ms
var alreadyCheckingSize = false;
var alreadyProcessing = false;
var date;
var gzFile, inflatedFile;
var logs = [];
var howmany = 1000;

var watcher = chokidar.watch(newPath, {
  ignored: /[\/\\]\./,
  persistent: true
});

watcher
  .on('error', (error) => { error('watcher error: ', error); })
  .on('add', (path) => { 
    log('added: ' + path);
    newFiles[path] = {};
    var stats = fs.statSync(path);
    newFiles[path].size = stats.size;
    setTimeout(hasSizeChanged, interval);
  })
  .on('change', (path, stats) => {
    if (stats) {
      //log('changed: ' + path + ' - size: ' + stats.size);
    }
});

function hasSizeChanged() {
  log('hasSizeChanged');

  if (alreadyCheckingSize)
    log('alreadyCheckingSize');
  else {
    alreadyCheckingSize = true;

    for (var path in newFiles) {
      var size = newFiles[path].size;
      var stats = fs.statSync(path);

      if (size === stats.size) {
        queuedFiles.push(path);
        log('pushed to queue: ' + path);
        delete newFiles[path];
        setTimeout(checkForQueuedFiles, interval);
      }
      else {
        log('changed: ' + path + ' - size: ' + stats.size);
        newFiles[path].size = stats.size;
        setTimeout(hasSizeChanged, interval);
      }
    }
    alreadyCheckingSize = false;
  }
}

function checkForQueuedFiles() {
  log('checkForQueuedFiles');

  if (alreadyProcessing)
    log('alreadyProcessing');
  else if (queuedFiles.length) {
    alreadyProcessing = true;
    startProcess(queuedFiles.shift());
  }
  else
    log('no queued files');
}

function startProcess(file) { 
  var start_process = process.hrtime(); //stats
  log('start processing: ' + file);
  date = file.split('_')[2];
  statsd.timing('start_process', getTime(start_process)); //stats
  inflateFile(file);
}

function inflateFile(file) {
  var inflate_file = process.hrtime(); //stats
  gzFile = file;
  inflatedFile = gzFile.replace('.gz', '');
  inflatedFile = inflatedFile.replace(newPath, inflatedPath);

  fs.readFile(gzFile, (err, data) => {
    if (err) 
      error(err);
    else {
      zlib.gunzip(data, (err, result) => {
        if (err) 
          error(err);
        else {
          fs.writeFileSync(inflatedFile, result);
          log('inflated: ' + inflatedFile);
          statsd.timing('inflate_file', getTime(inflate_file)); //stats

          loadFile(inflatedFile);
        }
      });
    }
  });
}

function loadFile(file) {
  var load_file = process.hrtime(); //stats
  
  const rl = readline.createInterface({ input: fs.createReadStream(file) });
  rl.on('line', (line) => { logs.push(line); });
  rl.on('close', () => {
    log('loaded: ' + file);
    statsd.timing('load_file', getTime(load_file)); //stats
    log('parsing: ' + file);
    parseLogs(logs.splice(0, howmany));
  });
}

function parseLogs(logs) {
  var parse_logs = process.hrtime(); //stats
  var bulk = [];

  for (var i=0; i < logs.length; i++) {
    var line = logs[i].split(' ');
    var url = line[6];

    var timestamp = line[3].substring(1).split(':');
    var d = new Date(timestamp[0]);

    var doc = {};
    doc.timestamp = d.toISOString().split('T')[0] + 'T' + timestamp.slice(1).join(':') + '.000Z';
    doc.ip = line[0];
    doc.code = line[8];
    doc.bytes = line[9];
    doc.referrer = line[10].replace(/"/g, '');
    doc.agent = line.slice(11).join(' ').split('"')[1];

    var action = {};
    var index = {};

    if (url.includes('media.imaginelearning.net')) {
      index._index = 'cdnlogs-media-' + date;
      url = url.replace(/80305E\/media\//, '');
    }
    else if (url.includes('ildc.cdn.imaginelearning.com')) {
      index._index = 'cdnlogs-ildccdn-' + date;
      url = url.replace(/80305E\/ildccdn\//, '');
    }
    else 
      index._index = 'cdnlogs-other-' + date;
    
    var extension = '-';

    var url_split = url.split('/');
    var file = url_split.slice(3).join('/');
    if (file.includes('.')) {
      if (file.includes('QualityLevels') || file.includes('Manifest')) {
        extension = 'ism';
        //console.log(file, extension);
      }
      else {
        var file_split = file.split('.'); 
        extension = file_split[file_split.length - 1].toLowerCase();
      }
    }

    doc.origin = url_split[2];
    doc.file = file;
    doc.extension = extension;
    doc.url = url;

    index._type = 'cdnlogs';
    action.index = index;
    bulk.push(action);
    bulk.push(doc);
  }

  statsd.timing('parse_logs', getTime(parse_logs)); //stats
  elasticBulk(bulk);
}

function elasticBulk(bulk) {
  var elastic_bulk = process.hrtime(); //stats

  elastic.bulk({ body: bulk }, (err, res) => {
    //statsd.timing('elastic_bulk', getTime(process.hrtime(elastic_bulk))); //stats
    statsd.timing('elastic_bulk', getTime(elastic_bulk)); //stats
    if (err) {
      statsd.increment('elastic_bulk_err'); //stats
      error(err);
    }
    else {
      //console.log(res);
      statsd.increment('elastic_bulk'); //stats
      if (logs.length > 0)
        parseLogs(logs.splice(0, howmany));
      else {
        log('finished processing: ' + inflatedFile);
        cleanUp();
      }
    }
    //process.stdout.write('remaining: ' + logs.length + '     \r');
  });
}

function cleanUp() {
  var clean_up = process.hrtime(); //stats

  fs.unlinkSync(inflatedFile);
  fs.renameSync(gzFile, gzFile.replace(newPath, processedPath));
  log('cleaned up: ' + gzFile);
  statsd.timing('clean_up', getTime(clean_up)); //stats

  if (queuedFiles.length)
    startProcess(queuedFiles.shift());
  else {
    //statsd.socket.close();
    log('no more files to process');
    alreadyProcessing = false;
  }
}


function log(message) {
  var data = new Date().toJSON() + ' - ' + message + '\n';

  fs.appendFile(logPath, data, (err) => {
    if (err) 
      error(err);
    else {
      //console.log('log appended');
    }
  });
}
function error(message) {
  var data = new Date().toJSON() + ' - ' + message + '\n';

  fs.appendFile(errorPath, data, (err) => {
    if (err) 
      console.error(err);
    else {
      //console.log('error appended');
    }
  });
}
function getTime(hrtimeObj) {
  var diff = process.hrtime(hrtimeObj);
  return Math.round(diff[0] * 1000 + diff[1] / 1000000);
}
statsd.socket.on('error', (err) => {
  error(err);
});
process.on('uncaughtException', (err) => {
  error(err);
});


/*

var clean_up = process.hrtime(); //stats
statsd.timing('clean_up', getTime(clean_up)); //stats

*/
