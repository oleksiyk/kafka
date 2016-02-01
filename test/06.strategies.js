'use strict';

/* global describe, it, before, sinon, after  */

var Promise = require('bluebird');
var Kafka   = require('../lib/index');
var _       = require('lodash');

var admin = new Kafka.GroupAdmin({ clientId: 'admin' });

before(function () {
    return admin.init();
});

after(function () {
    return admin.end();
});

describe('Weighted Round Robin Assignment', function () {
    var consumers = [
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-wrr',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer1'
        }),
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-wrr',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer2'
        })
    ];

    before(function () {
        return Promise.map(consumers, function (consumer, ind) {
            return consumer.init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                metadata: {
                    weight: ind + 1
                },
                fn: Kafka.WeightedRoundRobinAssignment
            });
        }).delay(200);
    });

    after(function () {
        this.timeout(5000);
        return Promise.map(consumers, function (c) {
            return c.end();
        }).delay(3000);
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
    var consumers = [
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-ring',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer1'
        }),
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-ring',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer2'
        })
    ];

    before(function () {
        return Promise.map(consumers, function (consumer, ind) {
            return consumer.init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                metadata: {
                    id: 'id' + ind,
                    weight: 10
                },
                fn: Kafka.ConsistentAssignment
            });
        }).delay(200);
    });

    after(function () {
        this.timeout(5000);
        return Promise.map(consumers, function (c) {
            return c.end();
        }).delay(3000);
    });

    it('should split partitions according to consumer weight', function () {
        return admin.describeGroup(consumers[0].options.groupId).then(function (group) {
            var consumer1 = _.find(group.members, { clientId: 'group-consumer1' });
            var consumer2 = _.find(group.members, { clientId: 'group-consumer2' });

            consumer1.memberAssignment.partitionAssignment[0].partitions.should.have.length(1);
            consumer1.memberAssignment.partitionAssignment[0].partitions.should.include(2);

            consumer2.memberAssignment.partitionAssignment[0].partitions.should.have.length(2);
            consumer2.memberAssignment.partitionAssignment[0].partitions.should.include(0);
            consumer2.memberAssignment.partitionAssignment[0].partitions.should.include(1);
        });
    });
});
