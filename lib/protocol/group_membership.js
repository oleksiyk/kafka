"use strict";

var protocol = require('bin-protocol');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol


//////////////////////////
// GROUP MEMBERSHIP API //
//////////////////////////

protocol.define('GroupCoordinatorRequest', {
    write: function(data) { // { groupId }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.GroupCoordinatorRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId);
    }
});

protocol.define('GroupCoordinatorResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .Int32BE('coordinatorId')
            .string('coordinatorHost')
            .Int32BE('coordinatorPort');
    }
});

protocol.define('JoinGroupRequest_GroupProtocolItem', {
    write: function(data) { // { name, metadata }
        this
            .string(data.name)
            .bytes(data.metadata);
    }
});

protocol.define('JoinGroupRequest', {
    write: function(data) { // { groupId sessionTimeout memberId protocolType groupProtocols }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.JoinGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.sessionTimeout)
            .string(data.memberId)
            .string(data.protocolType)
            .array(data.groupProtocols, this.JoinGroupRequest_GroupProtocolItem);
    }
});

// consumer protocol
protocol.define('JoinConsumerGroupRequest_GroupProtocolItem', {
    write: function(data) { // { assignmentStrategy, version, subscriptions, userData }
        var _o1, _o2;
        this
            .string(data.assignmentStrategy);
        _o1 = this.offset;
        this
            .skip(4) // following bytes length
            .Int16BE(data.version)
            .array(data.subscriptions, this.string)
            .bytes(data.userData);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

protocol.define('JoinConsumerGroupRequest', {
    write: function(data) { // { groupId sessionTimeout memberId groupProtocols }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.JoinGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.sessionTimeout)
            .string(data.memberId)
            .string('consumer')
            .array(data.groupProtocols, this.JoinConsumerGroupRequest_GroupProtocolItem);
    }
});

protocol.define('JoinGroupResponse_Member', {
    read: function() {
        this
            .string('id')
            .bytes('metadata');
    }
});

protocol.define('JoinGroupResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .Int32BE('generationId')
            .string('groupProtocol')
            .string('leaderId')
            .string('memberId')
            .array('members', this.JoinGroupResponse_Member);
    }
});

protocol.define('SyncGroupRequest_GroupAssignment', {
    write: function(data) { // { memberId memberAssignment }
        this
            .string(data.memberId)
            .bytes(data.memberAssignment);
    }
});

protocol.define('SyncGroupRequest', {
    write: function(data) { // { groupId generationId memberId groupAssignment }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.SyncGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .array(data.groupAssignment, this.SyncGroupRequest_GroupAssignment);
    }
});

// consumer protocol
protocol.define('SyncConsumerGroupRequest_PartitionAssignment', {
    write: function(data) { // { topic, partitions }
        this
            .string(data.topic)
            .array(data.partitions, this.Int32BE);
    },
    read: function () {
        this
            .string('topic')
            .array('partitions', this.Int32BE);
    }
});

protocol.define('SyncConsumerGroupRequest_MemberAssignment', {
    write: function(data) { // { version partitionAssignment userData }
        this
            .skip(4)
            .Int16BE(data.version)
            .array(data.partitionAssignment, this.SyncConsumerGroupRequest_PartitionAssignment)
            .bytes(data.userData);
    },
    read: function () {
        this
            .skip(4)
            .Int16BE('version')
            .array('partitionAssignment', this.SyncConsumerGroupRequest_PartitionAssignment)
            .bytes('userData');
    }
});

protocol.define('SyncConsumerGroupRequest_GroupAssignment', {
    write: function(data) { // { memberId memberAssignment}
        var _o1, _o2;
        this.string(data.memberId);
        _o1 = this.offset;
        this.SyncConsumerGroupRequest_MemberAssignment(data.memberAssignment);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

protocol.define('SyncConsumerGroupRequest', {
    write: function(data) { // { groupId generationId memberId groupAssignment }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.SyncGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .array(data.groupAssignment, this.SyncConsumerGroupRequest_GroupAssignment);
    }
});

protocol.define('SyncGroupResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .bytes('memberAssignment');
    }
});

// consumer protocol
protocol.define('SyncConsumerGroupResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .SyncConsumerGroupRequest_MemberAssignment('memberAssignment');
    }
});

protocol.define('HeartbeatRequest', {
    write: function(data) { // { groupId generationId memberId }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.HeartbeatRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .string(data.memberId);
    }
});

protocol.define('HeartbeatResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .ErrorCode('error');
    }
});

protocol.define('LeaveGroupRequest', {
    write: function(data) { // { groupId memberId }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.LeaveGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .string(data.memberId);
    }
});

protocol.define('LeaveGroupResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .ErrorCode('error');
    }
});
