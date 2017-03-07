'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

Protocol.define('ListGroupsRequest', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ListGroupsRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            });
    }
});

Protocol.define('ListGroupResponse_GroupItem', {
    read: function () {
        this
            .string('groupId')
            .string('protocolType');
    }
});

Protocol.define('ListGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .array('groups', this.ListGroupResponse_GroupItem);
    }
});

Protocol.define('DescribeGroupRequest', {
    write: function (data) {
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

Protocol.define('DescribeGroupResponse_ConsumerGroupMemberItem', {
    read: function () {
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

/* istanbul ignore next */
Protocol.define('DescribeGroupResponse_GroupMemberItem', {
    read: function () {
        this
            .string('memberId')
            .string('clientId')
            .string('clientHost')
            .bytes('memberMetadata')
            .bytes('memberAssignment');
    }
});

Protocol.define('DescribeGroupResponse_GroupItem', {
    read: function () {
        this
            .ErrorCode('error')
            .string('groupId')
            .string('state')
            .string('protocolType')
            .string('protocol');
        if (this.context.protocolType === 'consumer') {
            this.array('members', this.DescribeGroupResponse_ConsumerGroupMemberItem);
        } else {
            /* istanbul ignore next */
            this.array('members', this.DescribeGroupResponse_GroupMemberItem);
        }
    }
});

Protocol.define('DescribeGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('groups', this.DescribeGroupResponse_GroupItem);
    }
});
