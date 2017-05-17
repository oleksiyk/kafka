'use strict';

/* global describe, it, before, sinon, after  */

var Promise = require('bluebird');
var Kafka   = require('../lib/index');

describe('GroupAdmin', function () {
    var admin = new Kafka.GroupAdmin({ clientId: 'admin' });
    var consumer = new Kafka.GroupConsumer({
        groupId: 'no-kafka-admin-test-group',
        timeout: 1000,
        idleTimeout: 100,
        heartbeatTimeout: 100,
        clientId: 'group-consumer'
    });

    before(function () {
        return Promise.all([
            admin.init(),
            consumer.init({
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
            var found = false;

            groups.should.be.an('array').and.have.length.gt(0);
            groups.forEach(function (group) {
                if (group.groupId === consumer.options.groupId) {
                    group.should.have.property('protocolType', 'consumer');
                    found = true;
                }
            });
            found.should.equal(true);
        });
    });

    it('should describe group', function () {
        return admin.describeGroup(consumer.options.groupId).then(function (group) {
            group.should.be.an('object');
            group.should.have.property('error', null);
            group.should.have.property('groupId', consumer.options.groupId);
            group.should.have.property('state');
            group.should.have.property('protocolType', 'consumer');
            group.should.have.property('protocol', 'DefaultAssignmentStrategy');
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

describe('GroupAdmin', function () {
    var admin = new Kafka.GroupAdmin({ clientId: 'admin' });
    var consumer = new Kafka.GroupConsumer({
        groupId: 'no-kafka-admin-test-group',
        timeout: 1000,
        idleTimeout: 100,
        heartbeatTimeout: 100,
        clientId: 'group-consumer'
    });
    var producer = new Kafka.Producer({
        requiredAcks: 1,
        clientId: 'producer'
    });

    after(function () {
        return Promise.all([
            admin.end(),
            consumer.end(),
            producer.end()
        ]);
    });

    it('should fetch consumer lag', function () {
        return new Promise(function (resolve) {
            Promise.all([
                admin.init(),
                // Produce and consume a message in order to set
                // the current offset for partition 0
                consumer.init({
                    subscriptions: ['kafka-test-topic'],
                    metadata: 'consumer-metadata',
                    handler: function (messageSet, topic, partition) {
                        return Promise.each(messageSet, function (m) {
                            // commit offset
                            return consumer.commitOffset(
                                {
                                    topic: topic,
                                    partition: partition,
                                    offset: m.offset
                                }
                            );
                        }).then(function () {
                            // All messages consumed,
                            // expecting lag to be 0 at this point
                            resolve();
                        });
                    }
                }),
                producer.init()
            ]).then(function () {
                producer.send({
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        key: 'test-key',
                        value: 'Hello!'
                    }
                });
            });
        }).then(function () {
            return admin.fetchConsumerLag(consumer.options.groupId, [{
                topicName: 'kafka-test-topic',
                partitions: [0, 1, 2]
            }]);
        }).then(function (consumers) {
            consumers.should.have.length(3, 'asked for 3 partitions, expecting 3 responses');
            consumers[0].should.have.property('topic');
            consumers[0].should.have.property('partition');
            consumers[0].should.have.property('offset');
            consumers[0].should.have.property('highwaterMark');
            consumers[0].should.have.property('consumerLag');
            consumers[0].consumerLag.should.be.equal(0, 'expecting no lag, all messages are consumed');
        });
    });
});
