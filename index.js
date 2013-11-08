'use strict';

var path = require('path')
, basename = path.basename(path.dirname(__filename))
, debug = require('debug')('dm:contrib:' + basename)
, Transform = require("stream").Transform
, CSV = require('csv-string')
;


function notEmpty(x) {
  return (x !== '' && x !== undefined);
}


function Command(options)
{
  Transform.call(this, options);

  var self = this;
  self.begin = true;
  self.ended = false;
  self.keypath = options.keypath || null;
  self.database = options.id || 'csv2idb';
  self.objectstore = 'rows';
  self.title = !options.title ? false : true;
  self.delimiter = !options.delimiter ? "\n" : options.delimiter;
  self.titles = []
  self.separator = ',';
  self.buffer = '';
  self.countrows = 0;
  self.cr = "";

  self.header = ''
  self.header += '(function(){';
  self.header += self.cr;

  self.footer = '';
  self.footer += 'if(window.CustomEvent){';
  self.footer += self.cr;
  self.footer += 'var e=new CustomEvent("' + self.database + '",{detail:{message:"Data loaded",time:new Date()},bubbles:true,cancelable:true});';
  self.footer += self.cr;
  self.footer += 'document.dispatchEvent(e);';
  self.footer += self.cr;
	self.footer += '}';
  self.footer += self.cr;
  self.footer += '}';
  self.footer += self.cr;
  self.footer += 'var idb=window.indexedDB || window.mozIndexedDB || window.webkitIndexedDB || window.msIndexedDB;';
  self.footer += self.cr;
  self.footer += 'var r=idb.open("' + self.database + '");';
  self.footer += self.cr;
  self.footer += 'r.onupgradeneeded=initOS;';
  self.footer += self.cr;
  self.footer += 'r.onsuccess=function(e) {';
  self.footer += self.cr;
  self.footer += 'var d=r.result;';
  self.footer += self.cr;
  self.footer += 'var t=d.transaction("' + self.objectstore + '","readwrite");';
  self.footer += self.cr;
  self.footer += 'var s=t.objectStore("' + self.objectstore + '");';
  self.footer += self.cr;
  self.footer += 's.clear().onsuccess=function(e) {';
  self.footer += self.cr;
  self.footer += 'loadOS(d);';
  self.footer += self.cr;
  self.footer += '};';
  self.footer += self.cr;
  self.footer += '};';
  self.footer += self.cr;
  self.footer += '})();';
  self.footer += self.cr;

}

Command.prototype = Object.create(
  Transform.prototype, { constructor: { value: Command }});

Command.prototype.parse = function (rows, done) {
  var self = this;
  var res = rows.filter(notEmpty).map(function (row) {
      ++self.countrows;
      if (self.countrows === 1) {
        var code = '';
        code += 'function initOS(e){';
        code += self.cr;
        code += 'var o=e.currentTarget.result.createObjectStore("' + self.objectstore + '",';
        code += JSON.stringify({ keyPath: self.keypath, autoIncrement: (self.keypath ? false : true) });
        code += ');';
        code += self.cr;
        if (self.title) {
          self.titles = row.slice(0);
          code += self.titles.map(function (item) {
              if (self.keypath && self.keypath === item) {
                return;
              }
              return 'o.createIndex("' + item + '","' + item + '",{unique:false});';
            }
          ).join(self.cr);
        }
        code += self.cr;
        code += '}';
        code += self.cr;
        code += 'function loadOS(d){';
        code += self.cr;
        code += 'if(!d)return;';
        code += self.cr;
        code += 'var t=d.transaction("' + self.objectstore + '","readwrite");';
        code += self.cr;
        code += 'var s=t.objectStore("' + self.objectstore + '");';
        code += self.cr;
        code += "var r;";
        return code;
      }
      var dta = {};
      if (!self.title) {
        return 'r=s.put(' + JSON.stringify(row) + ');';
      }
      else if (self.title && row.length === self.titles.length) {
        self.titles.forEach(function (value, index) {
            if (row[index]) {
              dta[value] = row[index];
            }
          }
        )
        return 'r=s.put(' + JSON.stringify(dta) + ');';
      }
      else {
        return '';
      }
    }
  ).join(self.cr);
  self.push(res);
  done();
}


Command.prototype._transform = function (chunk, encoding, done) {
  var self = this;
  if (self.begin) {
    self.begin = false;
    self.separator = CSV.detect(chunk.toString());
    self.emit('begin');
    self.push(self.header);
  }
  self.buffer = self.buffer.concat(chunk.toString());
  var x = CSV.readChunk(self.buffer, self.separator, function (rows) {
      self.parse(rows, done);
    }
  );
  self.buffer = self.buffer.slice(x);
}

Command.prototype._flush = function (done) {
  var self = this;
  CSV.readAll(self.buffer, self.separator, function (rows) {
      self.parse(rows, function () {
          self.push(self.footer);
          done();
        }
      );
    }
  );
}


module.exports = function (options, si) {
  var cmd = new Command(options);
  return si.pipe(cmd);
}
