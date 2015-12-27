"use strict";

/* global describe, it, before, sinon  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.8 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var Promise = require('bluebird');
var Kafka   = require('../lib/index');
var _       = require('lodash');

var producer = new Kafka.Producer({requiredAcks: 1});
var consumer = new Kafka.GroupConsumer({
    idleTimeout: 100,
    heartbeatTimeout: 100
});

var dataListenerSpy = sinon.spy(function(messageSet, topic, partition) {
    messageSet.forEach(function (m) {
        consumer.commitOffset({topic: topic, partition: partition, offset: m.offset});
    });
});

consumer.on('data', dataListenerSpy);

describe('GroupConsumer', function () {
    before(function () {
        this.timeout(6000); // let Kafka create offset topic
        return Promise.all([
            producer.init(),
            consumer.init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                fn: Kafka.GroupConsumer.RoundRobinAssignment
            }).delay(1000)
        ]);
    });

    it('required methods', function () {
        return consumer.should
            .respondTo('init')
            .respondTo('subscribe')
            .respondTo('offset')
            .respondTo('unsubscribe')
            .respondTo('commitOffset')
            .respondTo('fetchOffset');
    });

    it('should receive new messages', function () {
        dataListenerSpy.reset();
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: {value: 'p00'}
        })
        .delay(200)
        .then(function () {
            /* jshint expr: true */
            dataListenerSpy.should.have.been.called;
            dataListenerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataListenerSpy.lastCall.args[1].should.be.a('string', 'kafka-test-topic');
            dataListenerSpy.lastCall.args[2].should.be.a('number', 0);

            dataListenerSpy.lastCall.args[0][0].should.be.an('object');
            dataListenerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataListenerSpy.lastCall.args[0][0].message.should.have.property('value');
            dataListenerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });

    it('should be able to commit offsets', function () {
        return consumer.commitOffset([
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
        return consumer.fetchOffset([
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
            consumer.offset('kafka-test-topic', 0),
            consumer.offset('kafka-test-topic', 1),
            consumer.offset('kafka-test-topic', 2),
        ]).spread(function (offset0, offset1, offset2) {
            offset0.should.be.a('number').and.be.gt(0);
            offset1.should.be.a('number').and.be.gt(0);
            offset2.should.be.a('number').and.be.gt(0);

            // commit them back for next tests
            return Promise.all([
                consumer.commitOffset({topic: 'kafka-test-topic', partition: 0, offset: offset0-1}),
                consumer.commitOffset({topic: 'kafka-test-topic', partition: 1, offset: offset1-1}),
                consumer.commitOffset({topic: 'kafka-test-topic', partition: 2, offset: offset2-1})
            ]);
        });
    });

    it('should split partitions in a group', function () {
        var consumer2 = new Kafka.GroupConsumer({
            idleTimeout: 100,
            heartbeatTimeout: 100
        }), consumer3 = new Kafka.GroupConsumer({
            idleTimeout: 100,
            heartbeatTimeout: 100
        });

        var dataListenerSpy2 = sinon.spy(function(messageSet, topic, partition) {
            messageSet.forEach(function (m) {
                consumer2.commitOffset({topic: topic, partition: partition, offset: m.offset});
            });
        });

        var dataListenerSpy3 = sinon.spy(function(messageSet, topic, partition) {
            messageSet.forEach(function (m) {
                consumer3.commitOffset({topic: topic, partition: partition, offset: m.offset});
            });
        });

        consumer2.on('data', dataListenerSpy2);
        consumer3.on('data', dataListenerSpy3);

        return Promise.all([
            consumer2.init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                fn: Kafka.GroupConsumer.RoundRobinAssignment
            }),
            consumer3.init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                fn: Kafka.GroupConsumer.RoundRobinAssignment
            }),
        ])
        .delay(500) // give some time to rebalance group
        .then(function () {
            dataListenerSpy.reset();
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
                dataListenerSpy.should.have.been.calledOnce;
                dataListenerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataListenerSpy2.should.have.been.calledOnce;
                dataListenerSpy2.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataListenerSpy3.should.have.been.calledOnce;
                dataListenerSpy3.lastCall.args[0].should.be.an('array').and.have.length(1);
            });
        });
    });

});
