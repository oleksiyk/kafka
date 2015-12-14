"use strict";

var protocol = require('bin-protocol');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

///////////////
// FETCH API //
///////////////

protocol.define('FetchRequestPartitionItem', {
    write: function(data) { // {partition, offset, maxBytes}
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .Int32BE(data.maxBytes);
    }
});

protocol.define('FetchRequestTopicItem', {
    write: function(data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array(data.partitions, this.FetchRequestPartitionItem);
    }
});

protocol.define('FetchRequest', {
    write: function(data) { // { timeout, minBytes, topics }
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

protocol.define('FetchResponseTopicItem', {
    read: function() {
        this
            .string('topicName')
            .array('partitions', this.FetchResponsePartitionItem);
    }
});

protocol.define('FetchResponsePartitionItem', {
    read: function() {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('highwaterMarkOffset')
            .Int32BE('messageSetSize')
            .MessageSet('messageSet', this.context.messageSetSize);
    }
});

protocol.define('FetchResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .array('topics', this.FetchResponseTopicItem);
    }
});
