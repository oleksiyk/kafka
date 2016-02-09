'use strict';

/* global describe, it, before, sinon, after  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.9 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var Promise = require('bluebird');
var Kafka   = require('../lib/index');
var _       = require('lodash');

var producer = new Kafka.Producer({ requiredAcks: 1, clientId: 'producer' });
var consumer = new Kafka.SimpleConsumer({ idleTimeout: 100, clientId: 'simple-consumer' });

var dataHandlerSpy = sinon.spy(function () {});

var maxBytesTestMessagesSize;

describe('SimpleConsumer', function () {
    before(function () {
        return Promise.all([
            producer.init(),
            consumer.init()
        ]);
    });

    after(function () {
        return Promise.all([
            producer.end(),
            consumer.end()
        ]);
    });

    it('required methods', function () {
        return consumer.should
            .respondTo('init')
            .respondTo('subscribe')
            .respondTo('offset')
            .respondTo('unsubscribe')
            .respondTo('commitOffset')
            .respondTo('fetchOffset')
            .respondTo('end');
    });

    it('should receive new messages', function () {
        return consumer.subscribe('kafka-test-topic', 0, dataHandlerSpy).then(function () {
            return producer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p00' }
            });
        })
        .delay(100)
        .then(function () {
            /* jshint expr: true */
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHandlerSpy.lastCall.args[1].should.be.a('string');
            dataHandlerSpy.lastCall.args[1].should.be.eql('kafka-test-topic');
            dataHandlerSpy.lastCall.args[2].should.be.a('number');
            dataHandlerSpy.lastCall.args[2].should.be.eql(0);
            dataHandlerSpy.lastCall.args[3].should.be.a('number');

            dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
            dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
            dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });

    it('should correctly encode/decode utf8 string message value', function () {
        dataHandlerSpy.reset();
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: '人人生而自由，在尊嚴和權利上一律平等。' }
        })
        .delay(100)
        .then(function () {
            /* jshint expr: true */
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHandlerSpy.lastCall.args[1].should.be.a('string');
            dataHandlerSpy.lastCall.args[1].should.be.eql('kafka-test-topic');
            dataHandlerSpy.lastCall.args[2].should.be.a('number');
            dataHandlerSpy.lastCall.args[2].should.be.eql(0);

            dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
            dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
            dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('人人生而自由，在尊嚴和權利上一律平等。');
        });
    });

    it('offset() should return last offset', function () {
        return consumer.offset('kafka-test-topic', 0).then(function (offset) {
            offset.should.be.a('number').and.be.gt(0);
        });
    });

    it('should receive messages from specified offset', function () {
        return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
            dataHandlerSpy.reset();
            return producer.send([{
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p000' }
            }, {
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p001' }
            }]);
        })
        .delay(100)
        .then(function () {
            return consumer.offset('kafka-test-topic', 0).then(function (offset) {
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset - 2 }, dataHandlerSpy)
                .delay(100) // consumer sleep timeout
                .then(function () {
                    /* jshint expr: true */
                    dataHandlerSpy.should.have.been.called; // eslint-disable-line
                    dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(2);
                    dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p000');
                    dataHandlerSpy.lastCall.args[0][1].message.value.toString('utf8').should.be.eql('p001');
                    // save for next test
                    maxBytesTestMessagesSize = dataHandlerSpy.lastCall.args[0][0].messageSize + dataHandlerSpy.lastCall.args[0][1].messageSize;
                });
            });
        });
    });

    it('should receive messages in maxBytes batches', function () {
        return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
            dataHandlerSpy.reset();
            return consumer.offset('kafka-test-topic', 0).then(function (offset) {
                // ask for maxBytes that is only 1 byte less then required for both last messages
                var maxBytes = 2 * (8 + 4) + maxBytesTestMessagesSize - 1;
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset - 2, maxBytes: maxBytes }, dataHandlerSpy)
                .delay(300)
                .then(function () {
                    /* jshint expr: true */
                    dataHandlerSpy.should.have.been.calledTwice; // eslint-disable-line
                    dataHandlerSpy.getCall(0).args[0].should.be.an('array').and.have.length(1);
                    dataHandlerSpy.getCall(1).args[0].should.be.an('array').and.have.length(1);
                    dataHandlerSpy.getCall(0).args[0][0].message.value.toString('utf8').should.be.eql('p000');
                    dataHandlerSpy.getCall(1).args[0][0].message.value.toString('utf8').should.be.eql('p001');
                });
            });
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
            },
            {
                topic: 'kafka-test-topic',
                partition: 2,
                offset: 3,
                metadata: 'm3'
            }
        ]).then(function (result) {
            result.should.be.an('array').that.has.length(3);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[2].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[1].should.have.property('topic', 'kafka-test-topic');
            result[2].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('partition').that.is.a('number');
            result[2].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('error', null);
            result[1].should.have.property('error', null);
            result[2].should.have.property('error', null);
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
            },
            {
                topic: 'kafka-test-topic',
                partition: 2
            }
        ]).then(function (result) {
            result.should.be.an('array').that.has.length(3);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[2].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[1].should.have.property('topic', 'kafka-test-topic');
            result[2].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('partition').that.is.a('number');
            result[2].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('offset').that.is.a('number');
            result[1].should.have.property('offset').that.is.a('number');
            result[2].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
            result[1].should.have.property('error', null);
            result[2].should.have.property('error', null);
            _.find(result, { topic: 'kafka-test-topic', partition: 0 }).offset.should.be.eql(1);
            _.find(result, { topic: 'kafka-test-topic', partition: 1 }).offset.should.be.eql(2);
            _.find(result, { topic: 'kafka-test-topic', partition: 2 }).offset.should.be.eql(3);
        });
    });
});
