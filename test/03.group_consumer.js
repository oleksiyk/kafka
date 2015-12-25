"use strict";

/* global describe, it, before, sinon  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.8 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var Promise = require('bluebird');
var Kafka = require('../lib/index');

var producer = new Kafka.Producer({requiredAcks: 1});
var consumer = new Kafka.GroupConsumer({
    idleTimeout: 100
});

var dataListenerSpy = sinon.spy(function() {});

consumer.on('data', dataListenerSpy);

describe('GroupConsumer', function () {
    before(function () {
        this.timeout(5000); // let Kafka create offset topic
        return Promise.all([
            producer.init(),
            consumer.init({
                strategy: 'TestStrategy',
                subscriptions: ['kafka-test-topic'],
                metadata: {
                    id: 'test-group-consumer',
                    weight: 50
                }
            }).delay(500)
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
        .delay(100)
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

    it('offset() should return last offset', function () {
        return consumer.offset('kafka-test-topic', 0).then(function (offset) {
            offset.should.be.a('number').and.be.gt(0);
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
            result.should.be.an('array').that.has.length(1);
            result[0].should.have.property('topicName', 'kafka-test-topic');
            result[0].should.have.property('partitions').that.is.an('array');
            result[0].partitions.should.have.length(2);
            result[0].partitions[0].should.be.an('object');
            result[0].partitions[0].should.have.property('partition', 0);
            result[0].partitions[0].should.have.property('error', null);
            result[0].partitions[1].should.have.property('partition', 1);
            result[0].partitions[1].should.have.property('error', null);
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
            result.should.be.an('array').that.has.length(1);
            result[0].should.have.property('topicName', 'kafka-test-topic');
            result[0].should.have.property('partitions').that.is.an('array');
            result[0].partitions.should.have.length(2);
            result[0].partitions[0].should.be.an('object');
            result[0].partitions[0].should.have.property('partition', 0);
            result[0].partitions[0].should.have.property('error', null);
            result[0].partitions[0].should.have.property('offset', 1);
            result[0].partitions[0].should.have.property('metadata', 'm1');
            result[0].partitions[1].should.have.property('partition', 1);
            result[0].partitions[1].should.have.property('error', null);
            result[0].partitions[1].should.have.property('offset', 2);
            result[0].partitions[1].should.have.property('metadata', 'm2');
        });
    });

});
