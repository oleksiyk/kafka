'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

/////////////////
// PRODUCE API //
/////////////////

Protocol.define('ProduceRequestPartitionItem', {
    write: function (data) { // {partition, messageSet}
        var _o1, _o2;
        this.Int32BE(data.partition);
        _o1 = this.offset;
        this
            .skip(4)
            .MessageSet(data.messageSet);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

Protocol.define('ProduceRequestTopicItem', {
    write: function (data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array(data.partitions, this.ProduceRequestPartitionItem);
    }
});

Protocol.define('ProduceRequest', {
    write: function (data) { // { requiredAcks, timeout, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .array(data.topics, this.ProduceRequestTopicItem);
    }
});

Protocol.define('ProduceResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.ProduceResponsePartitionItem);
    }
});

Protocol.define('ProduceResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('offset');
    }
});

Protocol.define('ProduceResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.ProduceResponseTopicItem)
            .Int32BE('throttleTime');
    }
});
