'use strict';

var protocol = require('bin-protocol');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

////////////////
// OFFSET API //
////////////////

protocol.define('OffsetRequestPartitionItem', {
    write: function (data) { // { partition, time, maxNumberOfOffsets }
        this
            .Int32BE(data.partition)
            .Int64BE(data.time) // Used to ask for all messages before a certain time (ms).
            .Int32BE(data.maxNumberOfOffsets);
    }
});

protocol.define('OffsetRequestTopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.OffsetRequestPartitionItem);
    }
});

protocol.define('OffsetRequest', {
    write: function (data) { // { topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1) // ReplicaId
            .array(data.topics, this.OffsetRequestTopicItem);
    }
});

protocol.define('OffsetResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.OffsetResponsePartitionItem);
    }
});

protocol.define('OffsetResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .array('offset', this.KafkaOffset);
    }
});

protocol.define('OffsetResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.OffsetResponseTopicItem);
    }
});
