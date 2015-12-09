"use strict";

var BinaryProtocol = require('binary-protocol');
var crc32          = require('buffer-crc32');
var errors         = require('./errors');

var protocol = new BinaryProtocol();

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
    read: function(propertyName) {
        this
            .pushStack({
                length: null,
                value: null
            })
            .Int32BE('length')
            .tap(function(data) {
                if (data.length <= 0) {
                    data.value = null;
                    return;
                }
                this.raw('value', data.length);
            })
            .popStack(propertyName, function(data) {
                return data.value;
            });
    },
    write: function(value) {
        if (!value) {
            this.Int32BE(-1);
        } else {
            this
                .Int32BE(value.length)
                .raw(value);
        }
    }
});

// variable length primitives, string
protocol.define('string', {
    read: function(propertyName) {
        this
            .pushStack({
                length: null,
                value: null
            })
            .Int16BE('length')
            .tap(function(data) {
                if (data.length === -1) {
                    data.value = null;
                    return;
                }
                this.raw('value', data.length);
            })
            .popStack(propertyName, function(data) {
                return data.value.toString('utf8');
            });
    },
    write: function(value) {
        if (!value) {
            this.Int16BE(-1);
        } else {
            this
                .allocate(2 + value.length)
                .Int16BE(value.length)
                .raw(new Buffer(value, 'utf8'));
        }
    }
});

// array
protocol.define('array', {
    read: function(primitive, propertyName) {
        this
            .pushStack({
                length: null,
                value: []
            })
            .Int32BE('length')
            .tap(function (data) {
                while(data.length-- > 0){
                    this[primitive]('value');
                }
            })
            .popStack(propertyName, function(data) {
                return data.value;
            });
    },
    write: function(primitive, value) {
        if (value === null) {
            this.Int32BE(-1);
        } else {
            this
                .Int32BE(value.length)
                .tap(function () {
                    var n = 0;
                    for (; n < value.length; n++){
                        this[primitive](value[n]);
                    }
                });
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
    read: function(propertyName) {
        this
            .pushStack({
                error: null
            })
            .Int16BE('error')
            .popStack(propertyName, function (data) {
                return errors.byCode(data.error);
            });
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
            .array('string', data.topicNames);
    }
});

protocol.define('Broker', {
    read: function(propertyName) {
        this
            .pushStack({
                nodeId: null,
                host: null,
                port: null
            })
            .Int32BE('nodeId')
            .string('host')
            .Int32BE('port')
            .popStack(propertyName);
    }
});

protocol.define('PartitionMetadata', {
    read: function(propertyName) {
        this
            .pushStack({
                error: null,
                partitionId: null,
                leader: null,
                replicas: null,
                isr: null
            })
            .ErrorCode('error')
            .Int32BE('partitionId')
            .Int32BE('leader')
            .array('Int32BE', 'replicas') // The set of alive nodes that currently acts as slaves for the leader for this partition
            .array('Int32BE', 'isr') // The set subset of the replicas that are "caught up" to the leader
            .popStack(propertyName);
    }
});

protocol.define('TopicMetadata', {
    read: function(propertyName) {
        this
            .pushStack({
                error: null,
                topicName: null,
                partitionMetadata: null
            })
            .ErrorCode('error')
            .string('topicName')
            .array('PartitionMetadata', 'partitionMetadata')
            .popStack(propertyName);
    }
});

protocol.define('MetadataResponse', {
    read: function() {
        this
            .pushStack({
                correlationId: null,
                broker: null,
                topicMetadata: null
            })
            .Int32BE('correlationId')
            .array('Broker', 'broker')
            .array('TopicMetadata', 'topicMetadata')
            .popStack('$')
            .end(function (r) {
                return r.$;
            });
    }
});

///////////////////////////////////////////
// END METADATA REQUEST AND RESPONSE END //
///////////////////////////////////////////


////////////////////////////
// MESSAGE AND MESSAGESET //
////////////////////////////

protocol.define('MessageAttributes', {
    read: function(propertyName) {
        this
            .pushStack({
                _raw: null,
                codec: null
            })
            .Int8('_raw')
            .tap(function(data) {
                data.codec = data._raw & 0x3;
            })
            .popStack(propertyName);
    },
    write: function(codec) {
        this
            .Int8(codec & 0x3);
    }
});

protocol.define('Message', {
    read: function(propertyName) {
        this
            .pushStack({
                crc: null,
                magicByte: null,
                attributes: null,
                key: null,
                value: null
            })
            .Int32BE('crc')
            .Int8('magicByte')
            .MessageAttributes('attributes')
            .bytes('key')
            .bytes('value')
            .popStack(propertyName);
    },
    write: function(value) { // {codec, magicByte, key, value}
        var _o1, _o2;
        this
            .tap(function () {_o1 = this.offset})
            .Int32BE(-1) // dumb value
            .Int8(value.magicByte || 0)
            .MessageAttributes(value.codec || 0)
            .bytes(value.key)
            .bytes(value.value)
            .tap(function () {
                _o2 = this.offset;
                this.offset = _o1;
                this.Int32BE(crc32.signed(this.buffer.slice(_o1+4, _o2)));
                this.offset = _o2;
            });
    }
});

protocol.define('MessageSetItem', {
    read: function() {
        this
            .Int64BE('offset')
            .Int32BE('messageSize')
            .Message('message');
    },
    write: function(value) { // {offset, message}
        var _o1, _o2;
        this
            .Int64BE(value.offset)
            .tap(function () {_o1 = this.offset})
            .Int32BE(-1) // dumb value
            .Message(value.message)
            .tap(function () {
                _o2 = this.offset;
                this.offset = _o1;
                this.Int32BE(_o2 - _o1 - 4);
                this.offset = _o2;
            });
    }
});

protocol.define('MessageSet', {
    read: function(size, propertyName) {
        var _o1;
        this
            .tap(function () {_o1 = this.offset})
            .pushStack({
                items: []
            })
            .loop('items', function () {
                if(this.offset - _o1 === size){ // stop when all messageSetSize bytes were consumed
                    return this.end();
                }
                this.MessageSetItem();
                if(this.next().AWAIT_NEXT === true){ // or stop when partial message found
                    this.ops = [];
                    return this.end();
                }
                return;
            })
            .popStack(propertyName, function(data) {
                return data.items;
            });
    },
    write: function(items) { // just an array of MessageSetItem
        this
            .tap(function () {
                var n = 0;
                for (; n < items.length; n++){
                    this.MessageSetItem(items[n]);
                }
            });
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
        this
            .Int32BE(data.partition)
            .tap(function () {_o1 = this.offset})
            .Int32BE(-1) // dumb value
            .MessageSet(data.messageSet)
            .tap(function () {
                _o2 = this.offset;
                this.offset = _o1;
                this.Int32BE(_o2 - _o1 - 4);
                this.offset = _o2;
            });
    }
});

protocol.define('ProduceRequestTopicItem', {
    write: function(data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array('ProduceRequestPartitionItem', data.partitions);
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
            .array('ProduceRequestTopicItem', data.topics);
    }
});

protocol.define('ProduceResponseTopicItem', {
    read: function(propertyName) {
        this
            .pushStack({
                topicName: null,
                partitions: null
            })
            .string('topicName')
            .array('ProduceResponsePartitionItem', 'partitions')
            .popStack(propertyName);
    }
});

protocol.define('ProduceResponsePartitionItem', {
    read: function(propertyName) {
        this
            .pushStack({
                partition: null,
                error: null,
                offset: null
            })
            .Int32BE('partition')
            .ErrorCode('error')
            .Int64BE('offset')
            .popStack(propertyName);
    }
});

protocol.define('ProduceResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .array('ProduceResponseTopicItem', 'topics');
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
            .array('FetchRequestPartitionItem', data.partitions);
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
            .array('FetchRequestTopicItem', data.topics);
    }
});

protocol.define('FetchResponseTopicItem', {
    read: function(propertyName) {
        this
            .pushStack({
                topicName: null,
                partitions: null
            })
            .string('topicName')
            .array('FetchResponsePartitionItem', 'partitions')
            .popStack(propertyName);
    }
});

protocol.define('FetchResponsePartitionItem', {
    read: function(propertyName) {
        this
            .pushStack({
                partition: null,
                error: null
            })
            .Int32BE('partition')
            .ErrorCode('error')
            .Int64BE('highwaterMarkOffset')
            .Int32BE('messageSetSize')
            .tap(function (data) {
                this.MessageSet(data.messageSetSize, 'messageSet');
            })
            .popStack(propertyName);
    }
});

protocol.define('FetchResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .array('FetchResponseTopicItem', 'topics');
    }
});
