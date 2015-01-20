console.log('Loading event');
var aws = require('aws-sdk');
var s3 = new aws.S3({apiVersion: '2006-03-01'});
var dynamodb = new aws.DynamoDB();
var tablename = 'lambda-output';
var username = 'samion';
var bucketprefix = 'fi.solita.lambda.' + username;

listBucket = function(bucket, callback) {
    var params = {
        Bucket: bucket
    };
    s3.listObjects(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else {
            console.log(data);           // successful response
            callback();
        }
    });
}

// adds file metadata to DynamoDB
addToDynamoDB = function(itemname, message, details, callback) {
    console.log('Adding ' + itemname + ' to DynamoDB');
    var item = {
        'username': { 'S': username },
        'date': { 'S': new Date().getTime().toString() },
        'isodate': { 'S': new Date().toISOString() }
    };
    if (message) item.message = { 'S': message };
    if (itemname) item.itemname = { 'S': itemname };
    if (details) item.details = { 'S': details };
    dynamodb.putItem({
        'TableName': tablename,
        'Item': item
    }, function(err, data) {
        if (err) {
            console.log(err, err.stack);
        } else {
            console.log(data);
        }
        callback();
    });
};

// handles the s3 file upload event
exports.handler = function(event, context) {
    console.log('Received event:');
    console.log(JSON.stringify(event, null, '  '));
    // Get the object from the event and show its content type
    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;
    
    listBucket(bucket, function() {
        console.log('Listed bucket ' + bucket);
    });

    // http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#getObject-property
    var params = {Bucket:bucket, Key:key};
    var message = '';
    var readable = s3.getObject(params).createReadStream();
    readable.setEncoding('utf8');
    readable.on('data', function(chunk) {
        message += chunk;
    });

    readable.on('end', function() {
        console.log('Content:\n' + message);
        var details = 'todo';
        addToDynamoDB(key, message, details, function() {
            console.log('Added to DynamoDB');
            context.done(null,'finished');
        });
    });
};
