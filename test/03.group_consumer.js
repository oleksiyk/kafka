'use strict';

/* global describe, it, before, sinon, after  */

var Promise = require('bluebird');
var Kafka   = require('../lib/index');
var _       = require('lodash');
var kafkaTestkit = require('./testkit/kafka');

function dataHandlerFactory(consumer) {
    return sinon.spy(function (messageSet, topic, partition) {
        return Promise.each(messageSet, function (m) {
            return consumer.commitOffset({ topic: topic, partition: partition, offset: m.offset });
        });
    });
}

describe('Group Consumer', function () {
    describe('Single topic', function () {
        var producer;
        var consumer;
        var dataHandlerSpy;

        before(function () {
            this.timeout(12000);
            producer = new Kafka.Producer({
                requiredAcks: 1,
                clientId: 'producer'
            });
            consumer = new Kafka.GroupConsumer({
                groupId: 'group-single-consumer',
                idleTimeout: 100,
                heartbeatTimeout: 100,
                clientId: 'group-consumer1'
            });
            dataHandlerSpy = dataHandlerFactory(consumer);

            return kafkaTestkit.createTopics([
                'kafka-group-consumer-topic-1'
            ]).then(function () {
                return Promise.all([
                    producer.init(),
                    consumer.init({
                        subscriptions: ['kafka-group-consumer-topic-1'],
                        handler: dataHandlerSpy
                    }),
                ]);
            });
        });

        after(function () {
            return Promise.all([
                producer.end(),
                consumer.end()
            ]);
        });

        afterEach(function () {
            dataHandlerSpy.reset();
        });

        it('require methods', function () {
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
            return producer.send({
                topic: 'kafka-group-consumer-topic-1',
                partition: 0,
                message: { value: 'p00' }
            })
            .delay(200)
            .then(function () {
                /* jshint expr: true */
                dataHandlerSpy.should.have.been.called; //eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', 'kafka-group-consumer-topic-1');
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
            });
        });

        it('should be able to commit offsets', function () {
            return consumer.commitOffset([
                {
                    topic: 'kafka-group-consumer-topic-1',
                    partition: 0,
                    offset: 1,
                    metadata: 'm1'
                },
                {
                    topic: 'kafka-group-consumer-topic-1',
                    partition: 1,
                    offset: 2,
                    metadata: 'm2'
                }
            ]).then(function (result) {
                result.should.be.an('array').that.has.length(2);
                result[0].should.be.an('object');
                result[1].should.be.an('object');
                result[0].should.have.property('topic', 'kafka-group-consumer-topic-1');
                result[1].should.have.property('topic', 'kafka-group-consumer-topic-1');
                result[0].should.have.property('partition').that.is.a('number');
                result[1].should.have.property('partition').that.is.a('number');
                result[0].should.have.property('error', null);
                result[1].should.have.property('error', null);
            });
        });

        it('should be able to fetch commited offsets', function () {
            return consumer.fetchOffset([
                {
                    topic: 'kafka-group-consumer-topic-1',
                    partition: 0
                },
                {
                    topic: 'kafka-group-consumer-topic-1',
                    partition: 1
                }
            ]).then(function (result) {
                result.should.be.an('array').that.has.length(2);
                result[0].should.be.an('object');
                result[1].should.be.an('object');
                result[0].should.have.property('topic', 'kafka-group-consumer-topic-1');
                result[1].should.have.property('topic', 'kafka-group-consumer-topic-1');
                result[0].should.have.property('partition').that.is.a('number');
                result[1].should.have.property('partition').that.is.a('number');
                result[0].should.have.property('offset').that.is.a('number');
                result[1].should.have.property('offset').that.is.a('number');
                result[0].should.have.property('metadata').that.is.a('string');
                result[1].should.have.property('metadata').that.is.a('string');
                result[0].should.have.property('error', null);
                result[1].should.have.property('error', null);
                _.find(result, { topic: 'kafka-group-consumer-topic-1', partition: 0 }).offset.should.be.eql(1 + 1);
                _.find(result, { topic: 'kafka-group-consumer-topic-1', partition: 1 }).offset.should.be.eql(2 + 1);
                _.find(result, { topic: 'kafka-group-consumer-topic-1', partition: 0 }).metadata.should.be.eql('m1');
                _.find(result, { topic: 'kafka-group-consumer-topic-1', partition: 1 }).metadata.should.be.eql('m2');
            });
        });

        it('offset() should return last offset', function () {
            return Promise.all([
                producer.send({
                    topic: 'kafka-group-consumer-topic-1',
                    partition: 0,
                    message: { value: 'p00' }
                }),
                producer.send({
                    topic: 'kafka-group-consumer-topic-1',
                    partition: 1,
                    message: { value: 'p00' }
                }),
                producer.send({
                    topic: 'kafka-group-consumer-topic-1',
                    partition: 2,
                    message: { value: 'p00' }
                }),
            ])
            .then(function () {
                return Promise.all([
                    consumer.offset('kafka-group-consumer-topic-1', 0),
                    consumer.offset('kafka-group-consumer-topic-1', 1),
                    consumer.offset('kafka-group-consumer-topic-1', 2),
                ]);
            })
            .spread(function (offset0, offset1, offset2) {
                offset0.should.be.a('number').and.be.gt(0);
                offset1.should.be.a('number').and.be.gt(0);
                offset2.should.be.a('number').and.be.gt(0);
            });
        });
    });

    describe('Multi topic', function () {
        var producer;
        var consumer;
        var dataHandlerSpy;

        before(function () {
            producer = new Kafka.Producer({
                requiredAcks: 1,
                clientId: 'producer'
            });
            consumer = new Kafka.GroupConsumer({
                groupId: 'group-multi-topic-consumer',
                idleTimeout: 100,
                heartbeatTimeout: 100,
                clientId: 'group-consumer1'
            });
            dataHandlerSpy = dataHandlerFactory(consumer);

            return kafkaTestkit.createTopics([
                'kafka-group-consumer-multi-topic-1',
                'kafka-group-consumer-multi-topic-2',
                'kafka-group-consumer-multi-topic-3',
            ]).then(function () {
                return Promise.all([
                    producer.init(),
                    consumer.init({
                        subscriptions: [
                            'kafka-group-consumer-multi-topic-1',
                            'kafka-group-consumer-multi-topic-2',
                            'kafka-group-consumer-multi-topic-3'
                        ],
                        handler: dataHandlerSpy
                    })
                ]);
            });
        });

        afterEach(function () {
            dataHandlerSpy.reset();
        });

        after(function () {
            return Promise.all([
                producer.end(),
                consumer.end()
            ]);
        });

        it('should receive messages from different topics for same instance', function () {
            return Promise.all([
                producer.send({
                    topic: 'kafka-group-consumer-multi-topic-1',
                    partition: 0,
                    message: { value: 'p01' }
                }),
                producer.send({
                    topic: 'kafka-group-consumer-multi-topic-2',
                    partition: 0,
                    message: { value: 'p02' }
                }),
                producer.send({
                    topic: 'kafka-group-consumer-multi-topic-3',
                    partition: 0,
                    message: { value: 'p03' }
                }),
            ])
            .delay(200)
            .then(function () {
                var topics = [];
                var messages = [];
                dataHandlerSpy.should.have.been.calledThrice; //eslint-disable-line
                topics = [
                    dataHandlerSpy.getCall(0).args[1],
                    dataHandlerSpy.getCall(1).args[1],
                    dataHandlerSpy.getCall(2).args[1],
                ];
                messages = [
                    dataHandlerSpy.getCall(0).args[0][0].message.value.toString('utf8'),
                    dataHandlerSpy.getCall(1).args[0][0].message.value.toString('utf8'),
                    dataHandlerSpy.getCall(2).args[0][0].message.value.toString('utf8'),
                ];
                topics.should.include('kafka-group-consumer-multi-topic-1');
                messages.should.include('p01');
                topics.should.include('kafka-group-consumer-multi-topic-2');
                messages.should.include('p02');
                topics.should.include('kafka-group-consumer-multi-topic-3');
                messages.should.include('p03');
            });
        });
    });

    describe('Multi consumer', function () {
        var producer;
        var firstConsumer;
        var secondConsumer;
        var thirdConsumer;
        var firstDataHandlerSpy;
        var secondDataHandlerSpy;
        var thirdDataHandlerSpy;

        before(function () {
            producer = new Kafka.Producer({
                requiredAcks: 1,
                clientId: 'producer'
            });
            firstConsumer = new Kafka.GroupConsumer({
                groupId: 'group-multi-consumer',
                idleTimeout: 100,
                heartbeatTimeout: 100,
                clientId: 'group-consumer1'
            });
            secondConsumer = new Kafka.GroupConsumer({
                groupId: 'group-multi-consumer',
                idleTimeout: 100,
                heartbeatTimeout: 100,
                clientId: 'group-consumer2'
            });
            thirdConsumer = new Kafka.GroupConsumer({
                groupId: 'group-multi-consumer',
                idleTimeout: 100,
                heartbeatTimeout: 100,
                clientId: 'group-consumer2'
            });
            firstDataHandlerSpy = dataHandlerFactory(firstConsumer);
            secondDataHandlerSpy = dataHandlerFactory(secondConsumer);
            thirdDataHandlerSpy = dataHandlerFactory(thirdConsumer);

            return kafkaTestkit.createTopics([
                'kafka-group-multi-consumer-topic-1',
            ]).then(function () {
                return Promise.all([
                    producer.init(),
                    firstConsumer.init({
                        subscriptions: ['kafka-group-multi-consumer-topic-1'],
                        handler: firstDataHandlerSpy,
                    }),
                    secondConsumer.init({
                        subscriptions: ['kafka-group-multi-consumer-topic-1'],
                        handler: secondDataHandlerSpy,
                    }),
                    thirdConsumer.init({
                        subscriptions: ['kafka-group-multi-consumer-topic-1'],
                        handler: thirdDataHandlerSpy,
                    }),
                ]);
            }).delay(1000); // give some time to rebalance group
        });

        afterEach(function () {
            firstDataHandlerSpy.reset();
            secondDataHandlerSpy.reset();
            thirdDataHandlerSpy.reset();
        });

        after(function () {
            return Promise.all([
                producer.end(),
                firstConsumer.end(),
                secondConsumer.end(),
                thirdConsumer.end(),
            ]);
        });

        it('should split partitions in a group', function () {
            return producer.send([
                {
                    topic: 'kafka-group-multi-consumer-topic-1',
                    partition: 0,
                    message: { value: 'p00' }
                },
                {
                    topic: 'kafka-group-multi-consumer-topic-1',
                    partition: 1,
                    message: { value: 'p01' }
                },
                {
                    topic: 'kafka-group-multi-consumer-topic-1',
                    partition: 2,
                    message: { value: 'p02' }
                }
            ])
            .delay(400)
            .then(function () {
                /* jshint expr: true */
                firstDataHandlerSpy.should.have.been.calledOnce; //eslint-disable-line
                firstDataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                secondDataHandlerSpy.should.have.been.calledOnce; //eslint-disable-line
                secondDataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                thirdDataHandlerSpy.should.have.been.calledOnce; //eslint-disable-line
                thirdDataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            });
        });
    });

    describe('Other', function () {
        it('should not log errors on clean shutdown', function () {
            var spy = sinon.spy(function () {});
            var consumer = new Kafka.GroupConsumer({
                groupId: 'no-kafka-shutdown-test-group',
                timeout: 1000,
                idleTimeout: 100,
                heartbeatTimeout: 100,
                logger: {
                    logFunction: spy
                },
                clientId: 'group-consumer4'
            });

            return consumer.init({
                subscriptions: ['kafka-test-topic'],
                handler: function () {}
            })
            .then(function () {
                spy.reset();
                return consumer.end();
            })
            .then(function () {
                /* jshint expr: true */
                spy.should.not.have.been.called; //eslint-disable-line
            });
        });

        it('should throw an error when groupId is invalid', function () {
            (function () {
                var c = new Kafka.GroupConsumer({
                    groupId: 'bad?group'
                });
                return c.init({
                    subscriptions: ['kafka-test-topic'],
                    handler: function () {}
                });
            }).should.throw('Invalid groupId');
        });
    });
});
