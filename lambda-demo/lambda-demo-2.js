console.log('Loading event');
var aws = require('aws-sdk');
var s3 = new aws.S3({apiVersion: '2006-03-01'});
var dynamodb = new aws.DynamoDB();
var tablename = 'device-event';
var username = 'samion';

// adds file metadata to DynamoDB
addToDynamoDB = function(devices, callback) {
    var items = [];
    var putrequests = [];
    for (var dev = 0; dev < devices.length; dev++) {
        var device = devices[dev];
        //console.log('Adding ' + device.device + ' to DynamoDB');
        var item = {
            'username': { 'S': username },
            'date': { 'S': new Date().getTime().toString() },
            'isodate': { 'S': new Date().toISOString() },
            'device' : { 'S': device.device },
            'events' : { 'S': device.count.toString() }
        };
        items.push(item);
        
        var putrequest = {
            'PutRequest': {
                'Item': item
            }
        };
        putrequests.push(putrequest);
    }
    
    //var batches = items.length;
    var batches = Math.ceil(items.length / 25);
    console.log('Creating ' + batches + ' DynamoDB batches of put item operations, total items ' + items.length);
    
    // latch used to wait for multiple callbacks to finish
    // promises could be used as well
    // simple countdown latch
    function CDL(countdown, completion) {
        this.signal = function() {
            if(--countdown < 1) completion(); 
        };
    }

    // usage
    var latch = new CDL(batches, function() {
        console.log('All ' + batches + ' DynamoDB operations finished.');
        callback();
    });
    
    /*for (var i = 0; i < items.length; i++) {
        putToDynamoDB(items[i], latch);
    }*/
    var start = 0;
    while (start < putrequests.length) {
        batchToDynamoDB(putrequests.slice(start, (Math.min(start + 25, putrequests.length))), latch);
        start += 24;
    }
};

putToDynamoDB = function(item, callback) {
    dynamodb.putItem({
            'TableName': tablename,
            'Item': item
    }, function(err, data) {
        if (err) console.log(err, err.stack);
        else console.log(data);
        callback.signal();
    });
}

batchToDynamoDB = function(items, callback) {
    dynamodb.batchWriteItem({
        'RequestItems': {
            'device-event': items
        }
    }, function(err, data) {
        if (err) console.log(err, err.stack);
        else console.log(data);
        callback.signal();
    });
}

// handles the s3 file upload event
exports.handler = function(event, context) {
    console.log('Received event:');
    console.log(JSON.stringify(event, null, '  '));
    // Get the object from the event and show its content type
    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;
    
    if (key.indexOf("part-") == -1) {
        context.done(null, 'nothing to do here');
    }
    
    // http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#getObject-property
    var params = {Bucket:bucket, Key:key};
    var message = '';
    var readable = s3.getObject(params).createReadStream();
    readable.setEncoding('utf8');
    readable.on('data', function(chunk) {
        message += chunk;
    });

    readable.on('end', function() {
        //console.log('Content:\n' + message);
        var lines = message.split("\n");
        var devices = [];
        for (var line = 0; line < lines.length; line++) {
            if (lines[line].length > 0) {
                //console.log('Line: ' + lines[line]);
                devices.push(JSON.parse(lines[line]));
            }
        }
        addToDynamoDB(devices, function() {
            console.log('Added to DynamoDB');
            context.done(null,'finished');
        });
    });
};
