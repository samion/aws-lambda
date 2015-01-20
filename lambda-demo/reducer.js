#!/usr/bin/env node
 
var events = require('events');
var emitter = new events.EventEmitter();
 
var remaining = '';
var lineReady = 'lineReady';
var dataReady = 'dataReady';
 
var deviceSummary = {
    device : '',
    count : 0
};
 
// escape all control characters so that they are plain text in the output
String.prototype.escape = function() {
    return this.replace('\n', '\\n').replace('\'', '\\\'').replace('\"', '\\"')
            .replace('\&', '\\&').replace('\r', '\\r').replace('\t', '\\t')
            .replace('\b', '\\b').replace('\f', '\\f');
}
 
// append an array to this one
Array.prototype.appendArray = function(arr) {
    this.push.apply(this, arr);
}
 
// data is complete, write it to the required output channel
emitter.on(dataReady, function(o) {
    if (o) {
        process.stdout.write(JSON.stringify(o) + '\n');
    }
});
 
// generate a JSON object from the captured input data, and then generate
// the required output
emitter.on(lineReady,function(data) {   
    if (!data || data == '') {
        // null data is probably a closing event, so emit a data ready
        emitter.emit(dataReady, deviceSummary);
        return;
    }
     
    try {
        obj = JSON.parse(data.split('\t')[1]);
    } catch (err) {
        process.stderr.write('Error Processing Line ' + data + '\n')
        process.stderr.write(err);
        return;
    }

    if (deviceSummary.device == '') {
        deviceSummary.device = obj.device;
        deviceSummary.count = 1;       
    } else {
        if (obj.device != deviceSummary.device) {
            // raise an event that the reduced array is completed
            emitter.emit(dataReady, deviceSummary);
            deviceSummary.device = obj.device;
            deviceSummary.count = 1;
        } else {
            deviceSummary.count += 1;
        }
    }
});
 
// fires on every block of data read from stdin
process.stdin.on('data', function(chunk) {
    var capture = chunk.split('\n');
 
    for (var i=0;i<capture.length; i++) {
        if (i==0) {
            emitter.emit(lineReady,remaining + capture[i]);
        } else if (i<capture.length-1) {
            emitter.emit(lineReady,capture[i]);
        } else {
            remaining = capture[i];
        }
    }
});
 
// fires when stdin is completed being read
process.stdin.on('end', function() {
    emitter.emit(lineReady,remaining);
});
 
// resume STDIN - paused by default
process.stdin.resume();
 
// set up the encoding for STDIN
process.stdin.setEncoding('utf8');
