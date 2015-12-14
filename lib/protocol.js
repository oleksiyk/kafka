"use strict";

var protocol = require('bin-protocol');
var crc32          = require('buffer-crc32');
var errors         = require('./errors');
// var _ = require('lodash');

/* jshint bitwise: false */

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

module.exports = protocol;

var API_KEYS = {
    ProduceRequest: 0,
    FetchRequest: 1,
    OffsetRequest: 2,
    MetadataRequest: 3,
    OffsetCommitRequest: 8,
    OffsetFetchRequest: 9,
    GroupCoordinatorRequest: 10,
    JoinGroupRequest: 11,
    HeartbeatRequest: 12,
    LeaveGroupRequest: 13,
    SyncGroupRequest: 14,
    DescribeGroupsRequest: 15,
    ListGroupsRequest: 16
};


//////////////////////////
// PRIMITIVE DATA TYPES //
//////////////////////////

// variable length primitives, bytes
protocol.define('bytes', {
    read: function() {
        this.Int32BE('length');
        if(this.context.length <= 0){
            return null;
        }
        this.raw('value', this.context.length);
        return this.context.value;
    },
    write: function(value) {
        if (value === undefined || value === null) {
            this.Int32BE(-1);
        } else {
            this
                .Int32BE(value.length)
                .raw(value);
        }
    }
});

// 64 bit offset, convert from Long (https://github.com/dcodeIO/long.js) to double, 53 bit
protocol.define('kafkaOffset', {
    read: function() {
        this.Int64BE('offset');
        return this.context.offset.toNumber();
    },
    write: function(value) {
        this.Int64BE(value);
    }
});

// variable length primitives, string
protocol.define('string', {
    read: function() {
        this.Int16BE('length');
        if(this.context.length <= 0){
            return null;
        }
        this.raw('value', this.context.length);
        return this.context.value.toString('utf8');
    },
    write: function(value) {
        if (value === undefined || value === null) {
            this.Int16BE(-1);
        } else {
            this
                .Int16BE(value.length)
                .raw(value);
        }
    }
});

// array
protocol.define('array', {
    read: function(fn) {
        this
            .Int32BE('length')
            .loop('items', fn, this.context.length);
        return this.context.items;
    },
    write: function(value, fn) {
        if (value === null || value === undefined) {
            this.Int32BE(-1);
        } else {
            this
                .Int32BE(value.length)
                .loop(value, fn);
        }
    }
});

//////////////////////////////////
// END PRIMITIVE DATA TYPES END //
//////////////////////////////////

protocol.define('RequestHeader', {
    write: function(header) {
        this
            .Int16BE(header.apiKey)
            .Int16BE(header.apiVersion)
            .Int32BE(header.correlationId)
            .string(header.clientId);
    }
});

protocol.define('ErrorCode', {
    read: function() {
        this.Int16BE('error');
        return errors.byCode(this.context.error);
    }
});

///////////////////////////////////
// METADATA REQUEST AND RESPONSE //
///////////////////////////////////

protocol.define('MetadataRequest', {
    write: function(data) { // data: { correlationId, clientId, [topicNames] }
        this
            .RequestHeader({
                apiKey: API_KEYS.MetadataRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.topicNames, this.string);
    }
});

protocol.define('Broker', {
    read: function() {
        this
            .Int32BE('nodeId')
            .string('host')
            .Int32BE('port');
    }
});

protocol.define('PartitionMetadata', {
    read: function() {
        this
            .ErrorCode('error')
            .Int32BE('partitionId')
            .Int32BE('leader')
            .array('replicas', this.Int32BE) // The set of alive nodes that currently acts as slaves for the leader for this partition
            .array('isr', this.Int32BE); // The set subset of the replicas that are "caught up" to the leader
    }
});

protocol.define('TopicMetadata', {
    read: function() {
        this
            .ErrorCode('error')
            .string('topicName')
            .array('partitionMetadata', this.PartitionMetadata);
    }
});

protocol.define('MetadataResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .array('broker', this.Broker)
            .array('topicMetadata', this.TopicMetadata);
    }
});

///////////////////////////////////////////
// END METADATA REQUEST AND RESPONSE END //
///////////////////////////////////////////


////////////////////////////
// MESSAGE AND MESSAGESET //
////////////////////////////

protocol.define('MessageAttributes', {
    read: function() {
        this.Int8('_raw');
        this.context.codec = this.context._raw & 0x3;
    },
    write: function(codec) {
        this.Int8(codec & 0x3);
    }
});

protocol.define('Message', {
    read: function() {
        // var _o1 = this.offset;
        this
            .Int32BE('crc')
            .Int8('magicByte')
            .MessageAttributes('attributes')
            .bytes('key')
            .bytes('value');
        // crc32.signed(this.buffer.slice(_o1+4, this.offset));
    },
    write: function(value) { // {codec, magicByte, key, value}
        var _o1 = this.offset, _o2;
        this
            .skip(4)
            .Int8(value.magicByte || 0)
            .MessageAttributes(value.codec || 0)
            .bytes(value.key)
            .bytes(value.value);

        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(crc32.signed(this.buffer.slice(_o1+4, _o2)));
        this.offset = _o2;
    }
});

protocol.define('MessageSetItem', {
    read: function(size, end) {
        if(size < 8 + 4){
            this.skip(size);
            return end();
        }
        this
            .kafkaOffset('offset')
            .Int32BE('messageSize');

        if(size < 8 + 4 + this.context.messageSize){
            this.skip(size - 8 - 4);
            return end();
        }

        this.Message('message');
    },
    write: function(value) { // {offset, message}
        var _o1, _o2;
        this.Int64BE(value.offset);
        _o1 = this.offset;
        this
            .skip(4)
            .Message(value.message);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

protocol.define('MessageSet', {
    read: function(size) {
        var _o1 = this.offset;
        if(!size){
            return [];
        }
        this.loop('items', function (end) {
            this.MessageSetItem(null, size - this.offset + _o1, end);
        });

        return this.context.items;
    },
    write: function(items) {
        this.loop(items, this.MessageSetItem);
    }
});

////////////////////////////////////
// END MESSAGE AND MESSAGESET END //
////////////////////////////////////


//////////////////////////////////
// PRODUCE REQUEST AND RESPONSE //
//////////////////////////////////

protocol.define('ProduceRequestPartitionItem', {
    write: function(data) { // {partition, messageSet}
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

protocol.define('ProduceRequestTopicItem', {
    write: function(data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array(data.partitions, this.ProduceRequestPartitionItem);
    }
});

protocol.define('ProduceRequest', {
    write: function(data) { // { requiredAcks, timeout, topics }
        this
            .RequestHeader({
                apiKey: API_KEYS.ProduceRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .array(data.topics, this.ProduceRequestTopicItem);
    }
});

protocol.define('ProduceResponseTopicItem', {
    read: function() {
        this
            .string('topicName')
            .array('partitions', this.ProduceResponsePartitionItem);
    }
});

protocol.define('ProduceResponsePartitionItem', {
    read: function() {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .kafkaOffset('offset');
    }
});

protocol.define('ProduceResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .array('topics', this.ProduceResponseTopicItem);
    }
});

//////////////////////////////////////////
// END PRODUCE REQUEST AND RESPONSE END //
//////////////////////////////////////////

////////////////////////////////
// FETCH REQUEST AND RESPONSE //
////////////////////////////////

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
                apiKey: API_KEYS.FetchRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(-1) // ReplicaId
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
            .kafkaOffset('highwaterMarkOffset')
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
