'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol


//////////////////
// METADATA API //
//////////////////

Protocol.define('MetadataRequest', {
    write: function (data) { // data: { correlationId, clientId, [topicNames] }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.topicNames, this.string);
    }
});

Protocol.define('Broker', {
    read: function () {
        this
            .Int32BE('nodeId')
            .string('host')
            .Int32BE('port');
    }
});

Protocol.define('PartitionMetadata', {
    read: function () {
        this
            .ErrorCode('error')
            .Int32BE('partitionId')
            .Int32BE('leader')
            .array('replicas', this.Int32BE) // The set of alive nodes that currently acts as slaves for the leader for this partition
            .array('isr', this.Int32BE); // The set subset of the replicas that are "caught up" to the leader
    }
});

Protocol.define('TopicMetadata', {
    read: function () {
        this
            .ErrorCode('error')
            .string('topicName')
            .array('partitionMetadata', this.PartitionMetadata);
    }
});

Protocol.define('MetadataResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('broker', this.Broker)
            .array('topicMetadata', this.TopicMetadata);
    }
});
