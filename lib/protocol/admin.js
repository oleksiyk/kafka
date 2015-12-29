"use strict";

var protocol = require('bin-protocol');
var globals  = require('./globals');

protocol.define('ListGroupsRequest', {
    write: function(data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ListGroupsRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            });
    }
});

protocol.define('ListGroupResponse_GroupItem', {
    read: function() {
        this
            .string('groupId')
            .string('protocolType');
    }
});

protocol.define('ListGroupResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .array('groups', this.ListGroupResponse_GroupItem);
    }
});

protocol.define('DescribeGroupRequest', {
    write: function(data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.DescribeGroupsRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.groups, this.string);
    }
});

protocol.define('DescribeGroupResponse_ConsumerGroupMemberItem', {
    read: function() {
        this
            .string('memberId')
            .string('clientId')
            .string('clientHost')
            .skip(4) // metadata bytes length
            .Int16BE('version')
            .array('subscriptions', this.string)
            .bytes('metadata')
            .SyncConsumerGroupRequest_MemberAssignment('memberAssignment');
    }
});

protocol.define('DescribeGroupResponse_GroupMemberItem', {
    read: function() {
        this
            .string('memberId')
            .string('clientId')
            .string('clientHost')
            .bytes('memberMetadata')
            .bytes('memberAssignment');
    }
});

protocol.define('DescribeGroupResponse_GroupItem', {
    read: function() {
        this
            .ErrorCode('error')
            .string('groupId')
            .string('state')
            .string('protocolType')
            .string('protocol');
        if(this.context.protocolType === 'consumer'){
            this.array('members', this.DescribeGroupResponse_ConsumerGroupMemberItem)
        } else {
            this.array('members', this.DescribeGroupResponse_GroupMemberItem)
        }
    }
});

protocol.define('DescribeGroupResponse', {
    read: function() {
        this
            .Int32BE('correlationId')
            .array('groups', this.DescribeGroupResponse_GroupItem);
    }
});
