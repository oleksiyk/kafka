'use strict';

/* global describe, it, before, sinon, after  */

var Promise = require('bluebird');
var Kafka   = require('../lib/index');
var _       = require('lodash');
var kafkaTestkit = require('./testkit/kafka');

var admin;

before(function () {
    admin = new Kafka.GroupAdmin({
        clientId: 'admin'
    });
    return admin.init();
});

after(function () {
    return admin.end();
});

describe('Weighted Round Robin Assignment', function () {
    var KAFKA_TOPIC = 'kafka-strategies-topic';
    var consumers = [
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-wrr',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer1',
        }),
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-wrr',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer2',
        })
    ];

    before(function () {
        return kafkaTestkit.createTopics([KAFKA_TOPIC]).then(function () {
            return Promise.map(consumers, function (consumer, ind) {
                return consumer.init({
                    subscriptions: [KAFKA_TOPIC],
                    metadata: {
                        weight: ind + 1
                    },
                    strategy: new Kafka.WeightedRoundRobinAssignmentStrategy(),
                    handler: function () {}
                });
            });
        })
        .delay(200);
    });

    after(function () {
        return Promise.map(consumers, function (c) {
            return c.end();
        });
    });

    it('should split partitions according to consumer weight', function () {
        return admin.describeGroup(consumers[0].options.groupId).then(function (group) {
            var consumer1 = _.find(group.members, { clientId: 'group-consumer1' });
            var consumer2 = _.find(group.members, { clientId: 'group-consumer2' });

            consumer1.memberAssignment.partitionAssignment[0].partitions.should.be.an('array').and.have.length(1);
            consumer2.memberAssignment.partitionAssignment[0].partitions.should.be.an('array').and.have.length(2);
        });
    });
});

describe('Consistent Assignment', function () {
    var KAFKA_TOPIC = 'kafka-consistent-assignment-test';
    var consumers = [
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-ring',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer1',
        }),
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-ring',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer2',
        })
    ];

    before(function () {
        return kafkaTestkit.createTopics([KAFKA_TOPIC]).then(function () {
            return Promise.map(consumers, function (consumer, ind) {
                return consumer.init({
                    subscriptions: [KAFKA_TOPIC],
                    metadata: {
                        id: 'id' + ind,
                        weight: 10
                    },
                    strategy: new Kafka.ConsistentAssignmentStrategy(),
                    handler: function () {}
                });
            });
        })
        .delay(200);
    });

    after(function () {
        return Promise.map(consumers, function (c) {
            return c.end();
        });
    });

    it('should split partitions according to consumer weight', function () {
        return admin.describeGroup(consumers[0].options.groupId).then(function (group) {
            var consumer1 = _.find(group.members, { clientId: 'group-consumer1' });
            var consumer2 = _.find(group.members, { clientId: 'group-consumer2' });
            var consumer1Partition = consumer1.memberAssignment.partitionAssignment[0].partitions[0];

            consumer1.memberAssignment.partitionAssignment[0].partitions.should.have.length(1);

            consumer2.memberAssignment.partitionAssignment[0].partitions.should.have.length(2);
            consumer2.memberAssignment.partitionAssignment[0].partitions.should.not.include(consumer1Partition);
        });
    });
});
