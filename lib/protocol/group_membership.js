'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol


//////////////////////////
// GROUP MEMBERSHIP API //
//////////////////////////

Protocol.define('GroupCoordinatorRequest', {
    write: function (data) { // { groupId }
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

Protocol.define('GroupCoordinatorResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .Int32BE('coordinatorId')
            .string('coordinatorHost')
            .Int32BE('coordinatorPort');
    }
});

/* istanbul ignore next */
Protocol.define('JoinGroupRequest_GroupProtocolItem', {
    write: function (data) { // { name, metadata }
        this
            .string(data.name)
            .bytes(data.metadata);
    }
});

/* istanbul ignore next */
Protocol.define('JoinGroupRequest', {
    write: function (data) { // { groupId sessionTimeout memberId protocolType groupProtocols }
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
Protocol.define('JoinConsumerGroupRequest_GroupProtocolItem', {
    write: function (data) { // { name, version, subscriptions, metadata }
        var _o1, _o2;
        this
            .string(data.name);
        _o1 = this.offset;
        this
            .skip(4) // following bytes length
            .Int16BE(data.version)
            .array(data.subscriptions, this.string)
            .bytes(data.metadata);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

Protocol.define('JoinConsumerGroupRequest', {
    write: function (data) { // { groupId sessionTimeout memberId groupProtocols }
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

/* istanbul ignore next */
Protocol.define('JoinGroupResponse_Member', {
    read: function () {
        this
            .string('id')
            .bytes('metadata');
    }
});

/* istanbul ignore next */
Protocol.define('JoinGroupResponse', {
    read: function () {
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

Protocol.define('JoinConsumerGroupResponse_Member', {
    read: function () {
        this
            .string('id')
            .skip(4)
            .Int16BE('version')
            .array('subscriptions', this.string)
            .bytes('metadata');
    }
});

Protocol.define('JoinConsumerGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .Int32BE('generationId')
            .string('groupProtocol')
            .string('leaderId')
            .string('memberId')
            .array('members', this.JoinConsumerGroupResponse_Member);
    }
});

/* istanbul ignore next */
Protocol.define('SyncGroupRequest_GroupAssignment', {
    write: function (data) { // { memberId memberAssignment }
        this
            .string(data.memberId)
            .bytes(data.memberAssignment);
    }
});

/* istanbul ignore next */
Protocol.define('SyncGroupRequest', {
    write: function (data) { // { groupId generationId memberId groupAssignment }
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
Protocol.define('SyncConsumerGroupRequest_PartitionAssignment', {
    write: function (data) { // { topic, partitions }
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

Protocol.define('SyncConsumerGroupRequest_MemberAssignment', {
    write: function (data) { // { version partitionAssignment metadata }
        this
            .skip(4)
            .Int16BE(data.version)
            .array(data.partitionAssignment, this.SyncConsumerGroupRequest_PartitionAssignment)
            .bytes(data.metadata);
    },
    read: function () {
        this.Int32BE('_blength');
        if (this.context._blength <= 0) {
            return null;
        }
        this.Int16BE('version')
            .array('partitionAssignment', this.SyncConsumerGroupRequest_PartitionAssignment)
            .bytes('metadata');
        return undefined;
    }
});

Protocol.define('SyncConsumerGroupRequest_GroupAssignment', {
    write: function (data) { // { memberId memberAssignment}
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

Protocol.define('SyncConsumerGroupRequest', {
    write: function (data) { // { groupId generationId memberId groupAssignment }
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

/* istanbul ignore next */
Protocol.define('SyncGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .bytes('memberAssignment');
    }
});

// consumer protocol
Protocol.define('SyncConsumerGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .SyncConsumerGroupRequest_MemberAssignment('memberAssignment');
    }
});

Protocol.define('HeartbeatRequest', {
    write: function (data) { // { groupId generationId memberId }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.HeartbeatRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId);
    }
});

Protocol.define('HeartbeatResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error');
    }
});

Protocol.define('LeaveGroupRequest', {
    write: function (data) { // { groupId memberId }
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

Protocol.define('LeaveGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error');
    }
});
