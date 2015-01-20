#!/usr/bin/env node
 
var events = require('events');
var emitter = new events.EventEmitter();

var line = '';
var lineEvent = 'line';
var dataReady = 'dataReady';
 
// escape all control characters so that they are plain text in the output
String.prototype.escape = function() {
    return this.replace('\n', '\\n').replace('\'', '\\\'').replace('\"', '\\"')
            .replace('\&', '\\&').replace('\r', '\\r').replace('\t', '\\t')
            .replace('\b', '\\b').replace('\f', '\\f');
}
 
// data is complete, write it to the required output channel
emitter.on(dataReady, function(arr) {
    var deviceEvent = {
	    device : arr[1],
        time : arr[2]
    }
    
    //process.stdout.write(arr[0] + '\n');
    process.stdout.write(deviceEvent.device + '\t' + JSON.stringify(deviceEvent) + '\n');
});
 
// find lines with text 'Laitteen tila on muuttunut' and output the related device and timestamp
emitter.on(lineEvent, function(l) {
    // skip line if it does not contain what we are interested in
    if (!l || l == '' || l.indexOf("Laitteen tila on muuttunut") == -1) {
        return;
    }

    // line is like:
    //
    // 00:04:17.654 | INFO  | f.l.t.o.u.l.LoggingRequestInterceptor | > <?xml version="1.0" encoding="UTF-8"?><syoteRivi tyyppi="TIEDON_MUOKKAUS"><kuvaus>Laitteen tila on muuttunut. Opastintaulussa 'Suorituskykytestaus 14 - Opastintaulu Hämeenlinnanväylä P - Kehä 3:n liittymä I' varoitusmerkkinä on '189' ja riveinä:
    //
    // Removing the 'Suorituskykytestaus 14 - ' portion of the device name
    var time = l.split(" ")[0];
    var devorig = l.split("'")[1];
    if (devorig == undefined) {
        //console.log('Broken line: ' + l);
        return;
    }
    if (devorig.indexOf(" - ") > -1) {
        device = '';
        var dev = devorig.split(" - ");
        for (var i = 1; i < dev.length; i++) {
            device += dev[i];
            if (i < dev.length - 1) {
                device += ' - ';
            }
        }
    } else {
        device = devorig;
    }
    
    emitter.emit(dataReady, [l, device, time]);
});
 
// fires on every block of data read from stdin
process.stdin.on('data', function(chunk) {
    // chunk and emit on newline
    lines = chunk.split("\n")
     
    if (lines.length > 0) {
        // append the first chunk to the existing buffer
        line += lines[0]
         
        if (lines.length > 1) {
            // emit the current buffer
            emitter.emit(lineEvent,line);
	    line = '';
 
            // go through the rest of the lines and emit them, buffering the last
            for (i=1; i<lines.length; i++) {
                if (i < lines.length) {
                    emitter.emit(lineEvent,lines[i]);
                } else {
                    line = lines[i];
                }
            }
        }
    }
});
 
// fires when stdin is completed being read
process.stdin.on('end', function() {
    emitter.emit(lineEvent,line);
});
 
// set up the encoding for STDIN
process.stdin.setEncoding('utf8');
 
// resume STDIN - paused by default
process.stdin.resume();
