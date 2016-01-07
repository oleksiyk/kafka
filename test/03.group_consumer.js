"use strict";

/* global describe, it, before, sinon  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.8 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var Promise = require('bluebird');
var Kafka   = require('../lib/index');
var _       = require('lodash');

var producer = new Kafka.Producer({requiredAcks: 1});
var consumers = [
    new Kafka.GroupConsumer({
        idleTimeout: 100,
        heartbeatTimeout: 100
    }),
    new Kafka.GroupConsumer({
        idleTimeout: 100,
        heartbeatTimeout: 100
    }),
    new Kafka.GroupConsumer({
        idleTimeout: 100,
        heartbeatTimeout: 100
    })
];

function listenerFactory (consumer){
    return sinon.spy(function (messageSet, topic, partition) {
        messageSet.forEach(function (m) {consumer.commitOffset({topic: topic, partition: partition, offset: m.offset}); });}
    );
}

var dataListenerSpies = [
    listenerFactory(consumers[0]),
    listenerFactory(consumers[1]),
    listenerFactory(consumers[2]),
];

consumers.forEach(function (c, i) {
    c.on('data', dataListenerSpies[i]);
});

describe('GroupConsumer', function () {
    before(function () {
        this.timeout(6000); // let Kafka create offset topic
        return Promise.all([
            producer.init(),
            consumers[0].init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                fn: Kafka.GroupConsumer.RoundRobinAssignment
            }).delay(1000)
        ]);
    });

    it('required methods', function () {
        return consumers[0].should
            .respondTo('init')
            .respondTo('subscribe')
            .respondTo('offset')
            .respondTo('unsubscribe')
            .respondTo('commitOffset')
            .respondTo('fetchOffset')
            .respondTo('end');
    });

    it('should receive new messages', function () {
        dataListenerSpies[0].reset();
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: {value: 'p00'}
        })
        .delay(200)
        .then(function () {
            /* jshint expr: true */
            dataListenerSpies[0].should.have.been.called;
            dataListenerSpies[0].lastCall.args[0].should.be.an('array').and.have.length(1);
            dataListenerSpies[0].lastCall.args[1].should.be.a('string', 'kafka-test-topic');
            dataListenerSpies[0].lastCall.args[2].should.be.a('number', 0);

            dataListenerSpies[0].lastCall.args[0][0].should.be.an('object');
            dataListenerSpies[0].lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataListenerSpies[0].lastCall.args[0][0].message.should.have.property('value');
            dataListenerSpies[0].lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });

    it('should be able to commit offsets', function () {
        return consumers[0].commitOffset([
            {
                topic: 'kafka-test-topic',
                partition: 0,
                offset: 1,
                metadata: 'm1'
            },
            {
                topic: 'kafka-test-topic',
                partition: 1,
                offset: 2,
                metadata: 'm2'
            }
        ]).then(function (result) {
            result.should.be.an('array').that.has.length(2);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[1].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('error', null);
            result[1].should.have.property('error', null);
        });
    });

    it('should be able to fetch commited offsets', function () {
        return consumers[0].fetchOffset([
        {
            topic: 'kafka-test-topic',
            partition: 0
        },
        {
            topic: 'kafka-test-topic',
            partition: 1
        }
        ]).then(function (result) {
            result.should.be.an('array').that.has.length(2);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[1].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('offset').that.is.a('number');
            result[1].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('metadata').that.is.a('string');
            result[1].should.have.property('metadata').that.is.a('string');
            result[0].should.have.property('error', null);
            result[1].should.have.property('error', null);
            _.find(result, {topic: 'kafka-test-topic', partition: 0}).offset.should.be.eql(1);
            _.find(result, {topic: 'kafka-test-topic', partition: 1}).offset.should.be.eql(2);
            _.find(result, {topic: 'kafka-test-topic', partition: 0}).metadata.should.be.eql('m1');
            _.find(result, {topic: 'kafka-test-topic', partition: 1}).metadata.should.be.eql('m2');
        });
    });

    it('offset() should return last offset', function () {
        return Promise.all([
            consumers[0].offset('kafka-test-topic', 0),
            consumers[0].offset('kafka-test-topic', 1),
            consumers[0].offset('kafka-test-topic', 2),
        ]).spread(function (offset0, offset1, offset2) {
            offset0.should.be.a('number').and.be.gt(0);
            offset1.should.be.a('number').and.be.gt(0);
            offset2.should.be.a('number').and.be.gt(0);

            // commit them back for next tests
            return Promise.all([
                consumers[0].commitOffset({topic: 'kafka-test-topic', partition: 0, offset: offset0-1}),
                consumers[0].commitOffset({topic: 'kafka-test-topic', partition: 1, offset: offset1-1}),
                consumers[0].commitOffset({topic: 'kafka-test-topic', partition: 2, offset: offset2-1})
            ]);
        });
    });

    it('should split partitions in a group', function () {
        this.timeout(6000);
        return Promise.all([
            consumers[1].init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                fn: Kafka.GroupConsumer.RoundRobinAssignment
            }),
            consumers[2].init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                fn: Kafka.GroupConsumer.RoundRobinAssignment
            }),
        ])
        .delay(500) // give some time to rebalance group
        .then(function () {
            dataListenerSpies[0].reset();
            return producer.send([
                {
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {value: 'p00'}
                },
                {
                    topic: 'kafka-test-topic',
                    partition: 1,
                    message: {value: 'p01'}
                },
                {
                    topic: 'kafka-test-topic',
                    partition: 2,
                    message: {value: 'p02'}
                }
            ])
            .delay(200)
            .then(function () {
                /* jshint expr: true */
                dataListenerSpies[0].should.have.been.calledOnce;
                dataListenerSpies[0].lastCall.args[0].should.be.an('array').and.have.length(1);
                dataListenerSpies[1].should.have.been.calledOnce;
                dataListenerSpies[1].lastCall.args[0].should.be.an('array').and.have.length(1);
                dataListenerSpies[2].should.have.been.calledOnce;
                dataListenerSpies[2].lastCall.args[0].should.be.an('array').and.have.length(1);
            });
        });
    });

    it('should list groups', function () {
        return consumers[0].listGroups().then(function (groups) {
            groups.should.be.an('array').and.have.length(1);
            groups[0].should.be.an('object');
            groups[0].should.have.property('groupId', consumers[0].options.groupId);
            groups[0].should.have.property('protocolType', 'consumer');
        });
    });

    it('should describe group', function () {
        return consumers[0].describeGroup().then(function (group) {
            group.should.be.an('object');
            group.should.have.property('error', null);
            group.should.have.property('groupId', consumers[0].options.groupId);
            group.should.have.property('state');
            group.should.have.property('protocolType', 'consumer');
            group.should.have.property('protocol', 'TestStrategy');
            group.should.have.property('members').that.is.an('array');
            group.members.should.have.length(3);
            group.members[0].should.be.an('object');
            group.members[0].should.have.property('memberId').that.is.a('string');
            group.members[0].should.have.property('clientId').that.is.a('string');
            group.members[0].should.have.property('clientHost').that.is.a('string');
            group.members[0].should.have.property('version').that.is.a('number');
            group.members[0].should.have.property('subscriptions').that.is.an('array');
            group.members[0].should.have.property('metadata');
            group.members[0].should.have.property('memberAssignment').that.is.a('object');
            group.members[0].memberAssignment.should.have.property('partitionAssignment').that.is.an('array');

        });
    });

});
