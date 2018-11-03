'use strict';
process.on('uncaughtException', (err) => { console.log(err); });

const fs = require('fs');
const src = process.argv[2];
const dst = process.argv[3];
var files_moved = 0;

console.log('src:', src, 'dst:', dst);


var files = fs.readdirSync(src);
console.log('#files:', files.length);

for (var i=0; i<files.length; i++) {
  var file = files[i];
  //if (file.startsWith('access_log-2010')) {
      //console.log(file);
      files_moved++;
      fs.renameSync(src + file, dst + file);
  //}

  process.stdout.write('files_moved: ' + files_moved + '     \r');
}

console.log('files_moved:', files_moved);
