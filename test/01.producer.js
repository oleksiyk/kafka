"use strict";

/* global describe, it, before  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.8 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var Kafka = require('../lib/index');

var producer = new Kafka.Producer({requiredAcks: 1});

describe('Producer', function () {

    before(function () {
        return producer.init();
    });

    it('required methods', function () {
        return producer.should
            .respondTo('init')
            .respondTo('send');
    });

    it('should send a single message', function () {
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: {
                value: 'Hello!'
            }
        }).then(function (result) {
            result.should.be.an('object');
            result.should.have.property('ok');
            result.should.have.property('errors');
            result.ok.should.be.an('array').and.have.length(1);
            result.errors.should.be.an('array').and.have.length(0);
            result.ok[0].should.be.an('object');
            result.ok[0].should.have.property('topic', 'kafka-test-topic');
            result.ok[0].should.have.property('partition', 0);
            result.ok[0].should.have.property('offset').that.is.a('number');
        });
    });

    it('should send an array of messages', function () {
        var msgs = [{
            topic: 'kafka-test-topic',
            partition: 1,
            message: {value: 'Hello!'}
        },{
            topic: 'kafka-test-topic',
            partition: 2,
            message: {value: 'Hello!'}
        }];
        return producer.send(msgs).then(function (result) {
            result.should.be.an('object');
            result.should.have.property('ok');
            result.should.have.property('errors');
            result.ok.should.be.an('array').and.have.length(2);
            result.errors.should.be.an('array').and.have.length(0);
            result.ok[0].should.be.an('object');
            result.ok[1].should.be.an('object');
            result.ok[0].should.have.property('topic', 'kafka-test-topic');
            result.ok[0].should.have.property('partition');
            result.ok[0].should.have.property('offset').that.is.a('number');
            result.ok[1].should.have.property('topic', 'kafka-test-topic');
            result.ok[1].should.have.property('partition');
            result.ok[1].should.have.property('offset').that.is.a('number');
        });
    });

});
