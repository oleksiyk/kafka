'use strict';

/* global describe, it, before, sinon, after  */

var Promise = require('bluebird');
var Kafka   = require('../lib/index');

var admin = new Kafka.GroupAdmin({ clientId: 'admin' });
var consumer = new Kafka.GroupConsumer({
    groupId: 'no-kafka-admin-test-group',
    timeout: 1000,
    idleTimeout: 100,
    heartbeatTimeout: 100,
    clientId: 'group-consumer'
});

describe('GroupAdmin', function () {
    before(function () {
        return Promise.all([
            admin.init(),
            consumer.init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                metadata: 'consumer-metadata',
                handler: function () {}
            })
        ]);
    });

    after(function () {
        return Promise.all([
            admin.end(),
            consumer.end()
        ]);
    });

    it('required methods', function () {
        return admin.should
            .respondTo('init')
            .respondTo('listGroups')
            .respondTo('describeGroup')
            .respondTo('end');
    });

    it('should list groups', function () {
        return admin.listGroups().then(function (groups) {
            groups.should.be.an('array').and.have.length(1);
            groups[0].should.be.an('object');
            groups[0].should.have.property('groupId', consumer.options.groupId);
            groups[0].should.have.property('protocolType', 'consumer');
        });
    });

    it('should describe group', function () {
        return admin.describeGroup(consumer.options.groupId).then(function (group) {
            group.should.be.an('object');
            group.should.have.property('error', null);
            group.should.have.property('groupId', consumer.options.groupId);
            group.should.have.property('state');
            group.should.have.property('protocolType', 'consumer');
            group.should.have.property('protocol', 'TestStrategy');
            group.should.have.property('members').that.is.an('array');
            group.members.should.have.length(1);
            group.members[0].should.be.an('object');
            group.members[0].should.have.property('memberId').that.is.a('string');
            group.members[0].should.have.property('clientId', consumer.options.clientId);
            group.members[0].should.have.property('clientHost').that.is.a('string');
            group.members[0].should.have.property('version').that.is.a('number');
            group.members[0].should.have.property('subscriptions').that.is.an('array');
            group.members[0].should.have.property('metadata');
            group.members[0].metadata.toString('utf8').should.be.eql('consumer-metadata');
            group.members[0].should.have.property('memberAssignment').that.is.a('object');
            group.members[0].memberAssignment.should.have.property('partitionAssignment').that.is.an('array');
            group.members[0].memberAssignment.partitionAssignment[0].should.have.property('topic', 'kafka-test-topic');
            group.members[0].memberAssignment.partitionAssignment[0].should.have.property('partitions').that.is.an('array', [0, 1, 2]);
        });
    });
});
