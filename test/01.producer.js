'use strict';

/* global describe, it, before, after  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.9 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var Kafka = require('../lib/index');

var producer = new Kafka.Producer({ requiredAcks: 1, clientId: 'producer' });

describe('Producer', function () {
    before(function () {
        return producer.init();
    });

    after(function () {
        return producer.end();
    });

    it('required methods', function () {
        return producer.should
            .respondTo('init')
            .respondTo('send')
            .respondTo('end');
    });

    it('should send a single message', function () {
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: {
                value: 'Hello!'
            }
        }).then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition', 0);
            result[0].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
        });
    });

    it('should send an array of messages', function () {
        var msgs = [{
            topic: 'kafka-test-topic',
            partition: 1,
            message: { value: 'Hello!' }
        }, {
            topic: 'kafka-test-topic',
            partition: 2,
            message: { value: 'Hello!' }
        }];
        return producer.send(msgs).then(function (result) {
            result.should.be.an('array').and.have.length(2);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('error', null);
            result[0].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('offset').that.is.a('number');
            result[1].should.have.property('topic', 'kafka-test-topic');
            result[1].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('offset').that.is.a('number');
            result[1].should.have.property('error', null);
        });
    });

    it('should return an error for unknown partition/topic and retry 3 times', function () {
        var start = Date.now(), msgs;
        this.timeout(4000);
        msgs = [{
            topic: 'kafka-test-unknown-topic',
            partition: 0,
            message: { value: 'Hello!' }
        }, {
            topic: 'kafka-test-topic',
            partition: 20,
            message: { value: 'Hello!' }
        }];
        return producer.send(msgs).then(function (result) {
            result.should.be.an('array').and.have.length(2);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[0].should.have.property('error');
            result[1].should.have.property('error');
            result[0].error.should.have.property('code', 'UnknownTopicOrPartition');
            result[1].error.should.have.property('code', 'UnknownTopicOrPartition');
            (Date.now() - start).should.be.closeTo(3 * 1000, 200);
        });
    });
});
