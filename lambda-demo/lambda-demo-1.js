console.log('Loading event');
var aws = require('aws-sdk');
var s3 = new aws.S3({apiVersion: '2006-03-01'});
var emr = new aws.EMR();
var username = 'samion';
var bucketprefix = 'fi.solita.lambda.' + username;

// create EMR job
createEMRJob = function(itemname, callback) {
    var d = new Date().getTime();
    var output = 's3://' + bucketprefix + '.emr.output/output-' + d;
    console.log('Creating EMR job for ' + itemname + '. Output goes to ' + output);
    var params = {
        Instances: {
            Ec2KeyName: 'bigdata',
            InstanceGroups: [{
                InstanceCount: 1,
                InstanceRole: 'MASTER',
                InstanceType: 'c3.xlarge',
                Market: 'ON_DEMAND',
                Name: 'Master instance group'
            }/*,
            {
                InstanceCount: 1,
                InstanceRole: 'CORE',
                InstanceType: 'c3.xlarge',
                Market: 'ON_DEMAND',
                Name: 'Core instance group'
            }*/],
            KeepJobFlowAliveWhenNoSteps: false,
            Placement: {
                AvailabilityZone: 'eu-west-1a'
            },
            TerminationProtected: false
        },
        Name: 'Device event counts by ' + username,
        AmiVersion: '3.6.0',
        BootstrapActions: [{
            Name: 'InstallNode.js',
            ScriptBootstrapAction: {
                Path: 's3://github-emr-bootstrap-actions/node/install-nodejs.sh'
            }
        }],
        JobFlowRole: 'emr-instance-role',
        LogUri: 's3://' + bucketprefix + '.emr.logs',
        ServiceRole: 'emr-service-role',
        Steps: [{
            HadoopJarStep: {
                Jar: '/home/hadoop/contrib/streaming/hadoop-streaming.jar',
                Args: [
                    '-files',
                    's3://' + bucketprefix + '.emr/mapper.js,s3://' + bucketprefix + '.emr/reducer.js',
                    '-input',
                    's3://' + bucketprefix + '.emr.input/' + itemname,
                    '-output',
                    output,
                    '-mapper',
                    'mapper.js',
                    '-reducer',
                    'reducer.js'
                ],
            },
            Name: 'NodeJSStreamProcess',
            ActionOnFailure: 'TERMINATE_CLUSTER'
        }],
        Tags: [{
            Key: 'name',
            Value: 'lambda'
        }],
        VisibleToAllUsers: true
    };
    emr.runJobFlow(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data);           // successful response
        callback();
    });
}

listBucket = function(bucket, callback) {
    var params = {
        Bucket: bucket
    };
    s3.listObjects(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data);           // successful response
        callback();
    });
}

copyLogFile = function(src, tgt, itemname, callback) {
    console.log('Copy ' + src + '/' + itemname + ' to ' + tgt);
    var params = {
        Bucket: tgt,
        CopySource: src + '/' + itemname,
        Key: itemname,
        ACL: 'public-read'
    };
    s3.copyObject(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data);           // successful response
        callback();
    });
}

// handles the s3 file upload event
exports.handler = function(event, context) {
    console.log('Received event:');
    console.log(JSON.stringify(event, null, '  '));
    // Get the object from the event and show its content type
    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;
    
    var src = bucketprefix + '.input';
    var tgt = bucketprefix + '.emr.input';
    
    listBucket(src, function() {
        console.log('Listed bucket ' + src);
        
        listBucket(tgt, function() {
            console.log('Listed bucket ' + tgt);
            
            copyLogFile(src, tgt, key, function() {
                console.log('Log file copied');
                createEMRJob(key, function() {
                    console.log('EMR job created');
                    context.done(null,'finished successfully');
                });
            });
        });
    });
};
