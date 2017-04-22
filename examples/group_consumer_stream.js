'use strict';

var JSONStream = require('JSONStream');
var through2 = require('through2');
var kafka = require('../');

var config = {
    connectionString: 'localhost:9092',
    groupId: 'kafka-test-group-id',
    topic: 'kafka-test-topic'
};

/**
 * You can queue test-messages via:
 * /usr/local/bin/kafka-console-producer --broker-list localhost:9092 --topic kafka-test-topic < ./test-message.json
 */
var consumerGroupStream = new kafka.GroupConsumerStream({
    groupId: config.groupId,
    connectionString: config.connectionString,
});
var stream = consumerGroupStream.getStream([{
    subscriptions: [config.topic]
}]);

stream
    .on('error', function (err) {
        // eslint-disable-next-line no-console
        console.error(err, err.stack);
        process.exit(1);
    })
    // if you don't call this, you'll need to .pipe and commit the offsets yourself
    .pipe(consumerGroupStream.commitOffsetTransform())
    .pipe(through2.obj(function (data, enc, callback) {
        data.messageSet.forEach(function (m) {
            callback(null, m.message.value.toString());
        });
    }))
    .pipe(JSONStream.parse())
    // you can do some processing here
    .pipe(through2.obj(function (message, enc, callback) {
        callback(null, message.name);
    }))
    .pipe(process.stdout);
