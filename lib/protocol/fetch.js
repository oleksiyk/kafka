'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

///////////////
// FETCH API //
///////////////

Protocol.define('FetchRequestPartitionItem', {
    write: function (data) { // {partition, offset, maxBytes}
        this
            .Int32BE(data.partition)
            .KafkaOffset(data.offset)
            .Int32BE(data.maxBytes);
    }
});

Protocol.define('FetchRequestTopicItem', {
    write: function (data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array(data.partitions, this.FetchRequestPartitionItem);
    }
});

Protocol.define('FetchRequest', {
    write: function (data) { // { timeout, minBytes, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.FetchRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1) // ReplicaId
            .Int32BE(data.timeout)
            .Int32BE(data.minBytes)
            .array(data.topics, this.FetchRequestTopicItem);
    }
});

Protocol.define('FetchResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.FetchResponsePartitionItem);
    }
});

Protocol.define('FetchResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('highwaterMarkOffset')
            .Int32BE('messageSetSize')
            .MessageSet('messageSet', this.context.messageSetSize);
    }
});

Protocol.define('FetchResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.FetchResponseTopicItem);
    }
});
