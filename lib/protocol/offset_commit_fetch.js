'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

/////////////////////////////
// OFFSET COMMIT/FETCH API //
/////////////////////////////

// group coordinator request and response are defined in group_membership.js

// v0
Protocol.define('OffsetCommitRequestV0PartitionItem', {
    write: function (data) { // { partition, offset, metadata }
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .string(data.metadata);
    }
});

Protocol.define('OffsetCommitRequestV0TopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.OffsetCommitRequestV0PartitionItem);
    }
});

Protocol.define('OffsetCommitRequestV0', {
    write: function (data) { // { groupId, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .array(data.topics, this.OffsetCommitRequestV0TopicItem);
    }
});

// v1
/* istanbul ignore next */
Protocol.define('OffsetCommitRequestV1PartitionItem', {
    write: function (data) { // { partition, offset, timestamp, metadata }
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .Int64BE(data.timestamp)
            .string(data.metadata);
    }
});

/* istanbul ignore next */
Protocol.define('OffsetCommitRequestV1TopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.OffsetCommitRequestV1PartitionItem);
    }
});

/* istanbul ignore next */
Protocol.define('OffsetCommitRequestV1', {
    write: function (data) { // { groupId, generationId, memberId,  topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .string(data.generationId)
            .string(data.memberId)
            .array(data.topics, this.OffsetCommitRequestV1TopicItem);
    }
});

// v2
Protocol.define('OffsetCommitRequestV2', {
    write: function (data) { // { groupId, generationId, memberId, retentionTime, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .Int64BE(data.retentionTime)
            .array(data.topics, this.OffsetCommitRequestV0TopicItem); // same as in v0
    }
});


Protocol.define('OffsetCommitResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.OffsetCommitResponsePartitionItem);
    }
});

Protocol.define('OffsetCommitResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error');
    }
});

// v0, v1 and v2
Protocol.define('OffsetCommitResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.OffsetCommitResponseTopicItem);
    }
});


Protocol.define('OffsetFetchRequestTopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.Int32BE);
    }
});

Protocol.define('OffsetFetchRequest', {
    write: function (data) { // { apiVersion, groupId, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetFetchRequest,
                apiVersion: data.apiVersion || 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .array(data.topics, this.OffsetFetchRequestTopicItem);
    }
});

Protocol.define('OffsetFetchResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.OffsetFetchResponsePartitionItem);
    }
});

Protocol.define('OffsetFetchResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .KafkaOffset('offset')
            .string('metadata')
            .ErrorCode('error');
    }
});

Protocol.define('OffsetFetchResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.OffsetFetchResponseTopicItem);
    }
});
