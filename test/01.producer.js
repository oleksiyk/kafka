'use strict';

/* global describe, it, before, after, sinon, expect  */

var util    = require('util');
var Promise = require('bluebird');
var Kafka   = require('../lib/index');

var DefaultPartitioner = Kafka.DefaultPartitioner;
var producer;

var kafkaTestkit = require('./testkit/kafka');

describe('Producer', function () {
    before(function () {
        producer = new Kafka.Producer({
            requiredAcks: 1,
            clientId: 'producer',
        });
        return Promise.all([
            kafkaTestkit.createTopics([
                'kafka-producer-topic-1',
                'kafka-producer-topic-2',
                'kafka-producer-topic-3',
            ]),
            producer.init(),
        ]);
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

    it('should create producer with default options', function () {
        var _producer = new Kafka.Producer(); // eslint-disable-line
    });

    it('should send a single message', function () {
        return producer.send({
            topic: 'kafka-producer-topic-1',
            partition: 0,
            message: {
                value: 'Hello!'
            }
        }).then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-producer-topic-1');
            result[0].should.have.property('partition', 0);
            result[0].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
        });
    });

    it('should send a single keyed message', function () {
        return producer.send({
            topic: 'kafka-producer-topic-1',
            partition: 0,
            message: {
                key: 'test-key',
                value: 'Hello!'
            }
        }).then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-producer-topic-1');
            result[0].should.have.property('partition', 0);
            result[0].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
        });
    });

    it('should fail when missing topic field', function () {
        return producer.send({
            partition: 0,
            message: {
                value: 'Hello!'
            }
        }).should.eventually.be.rejectedWith('Missing or wrong topic field');
    });

    it('should use default partitioner when missing partition field and no partitioner function defined', function () {
        return producer.send({
            topic: 'kafka-producer-topic-1',
            message: {
                value: 'Hello!'
            }
        }).then(function (result) {
            result.should.be.an('array').and.have.length(1);
        });
    });

    it('should send an array of messages', function () {
        var msgs = [{
            topic: 'kafka-producer-topic-1',
            partition: 1,
            message: { value: 'Hello!' }
        }, {
            topic: 'kafka-producer-topic-1',
            partition: 2,
            message: { value: 'Hello!' }
        }];
        return producer.send(msgs).then(function (result) {
            result.should.be.an('array').and.have.length(2);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-producer-topic-1');
            result[0].should.have.property('error', null);
            result[0].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('offset').that.is.a('number');
            result[1].should.have.property('topic', 'kafka-producer-topic-1');
            result[1].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('offset').that.is.a('number');
            result[1].should.have.property('error', null);
        });
    });

    it('should send messages to multiple topics with the same instance', function () {
        var msgs = [{
            topic: 'kafka-producer-topic-1',
            partition: 1,
            message: { value: 'Hello!' }
        }, {
            topic: 'kafka-producer-topic-2',
            partition: 2,
            message: { value: 'Hello!' }
        }, {
            topic: 'kafka-producer-topic-3',
            partition: 2,
            message: { value: 'Hello!' }
        }];
        return producer.send(msgs).then(function (result) {
            var resultTopics = [result[0].topic, result[1].topic, result[2].topic];

            result.should.be.an('array').and.have.length(3);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[2].should.be.an('object');

            resultTopics.should.include('kafka-producer-topic-1');
            resultTopics.should.include('kafka-producer-topic-2');
            resultTopics.should.include('kafka-producer-topic-3');

            result[0].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
            result[1].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('offset').that.is.a('number');
            result[1].should.have.property('error', null);
            result[2].should.have.property('partition').that.is.a('number');
            result[2].should.have.property('offset').that.is.a('number');
            result[2].should.have.property('error', null);
        });
    });

    it('should return an error for unknown partition/topic', function () {
        const unknownTopicProducer = new Kafka.Producer({
            requiredAcks: 1,
            clientId: 'producer'
        });

        var msgs = [{
            topic: 'kafka-test-unknown-topic',
            partition: 0,
            message: { value: 'Hello!' }
        }];
        return unknownTopicProducer.send(msgs, {
            retries: {
                attempts: 5,
                delay: {
                    min: 100,
                    max: 300
                }
            }
        }).should.eventually.be.rejectedWith('This request is for a topic or partition that does not exist on this broker.');
    });

    it('partitioner arguments', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer'
        });
        var partitionerSpy = _producer.partitioner.partition = sinon.spy(function () { return 1; });
        return _producer.init().then(function () {
            return _producer.send({
                topic: 'kafka-producer-topic-1',
                message: {
                    value: 'Hello!'
                }
            });
        })
        .then(function () {
            partitionerSpy.should.have.been.called; // eslint-disable-line
            partitionerSpy.lastCall.args[0].should.be.a('string').that.is.eql('kafka-producer-topic-1');
            partitionerSpy.lastCall.args[1].should.be.an('array').and.have.length(3);
            partitionerSpy.lastCall.args[1][0].should.be.an('object');
            partitionerSpy.lastCall.args[1][0].should.have.property('partitionId').that.is.a('number');
            partitionerSpy.lastCall.args[1][0].should.have.property('error').that.is.eql(null);
            partitionerSpy.lastCall.args[1][0].should.have.property('leader').that.is.a('number');
            partitionerSpy.lastCall.args[1][0].should.have.property('replicas').that.is.an('array');
            partitionerSpy.lastCall.args[1][0].should.have.property('isr').that.is.an('array');
        });
    });

    it('should throw when partitioner is not a DefaultPartitioner', function () {
        function _try() {
            return new Kafka.Producer({
                clientId: 'producer',
                partitioner: 'something else'
            });
        }

        expect(_try).to.throw('Partitioner must inherit from Kafka.DefaultPartitioner');
    });

    it('should determine topic partition using inherited partitioner', function () {
        var _producer;

        function MyPartitioner() {}
        util.inherits(MyPartitioner, DefaultPartitioner);

        MyPartitioner.prototype.partition = function dummySyncPartitioner(/*topicName, partitions, message*/) {
            return 1;
        };

        _producer = new Kafka.Producer({
            clientId: 'producer',
            partitioner: new MyPartitioner()
        });

        return _producer.init().then(function () {
            return _producer.send({
                topic: 'kafka-producer-topic-1',
                message: {
                    value: 'Hello!'
                }
            });
        })
        .then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-producer-topic-1');
            result[0].should.have.property('partition', 1);
            result[0].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
        });
    });

    it('should determine topic partition using async partitioner function', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer'
        });
        _producer.partitioner.partition = function dummySyncPartitioner(/*topicName, partitions, message*/) {
            return Promise.delay(100).then(function () {
                return 2;
            });
        };
        return _producer.init().then(function () {
            return _producer.send({
                topic: 'kafka-producer-topic-1',
                message: {
                    value: 'Hello!'
                }
            });
        })
        .then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-producer-topic-1');
            result[0].should.have.property('partition', 2);
            result[0].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
        });
    });

    it('should return error for unknown topic', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer'
        });
        return _producer.init().then(function () {
            return _producer.send({
                topic: 'kafka-test-unknown-topic',
                message: {
                    value: 'Hello!'
                }
            }, {
                retries: {
                    attempts: 1
                }
            });
        })
        .should.eventually.be.rejectedWith('This request is for a topic or partition that does not exist on this broker.');
    });

    it('should group messages by global batch.size', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer',
            batch: {
                size: 10
            }
        });

        var spy = sinon.spy(_producer.client, 'produceRequest');

        return _producer.init().then(function () {
            return Promise.all([
                _producer.send({
                    topic: 'kafka-producer-topic-1',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }),
                _producer.send({
                    topic: 'kafka-producer-topic-1',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                })
            ]);
        })
        .then(function () {
            spy.should.have.been.calledOnce; // eslint-disable-line
        });
    });

    it('should not group messages with size > batch.size', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer',
            batch: {
                size: 1
            }
        });

        var spy = sinon.spy(_producer.client, 'produceRequest');

        return _producer.init().then(function () {
            return Promise.all([
                _producer.send({
                    topic: 'kafka-producer-topic-1',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }),
                _producer.send({
                    topic: 'kafka-producer-topic-1',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                })
            ]);
        })
        .then(function () {
            spy.should.have.been.calledTwice; // eslint-disable-line
        });
    });

    it('should group messages by batch.size in options', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer',
            batch: {
                size: 1
            }
        });

        var spy = sinon.spy(_producer.client, 'produceRequest');

        return _producer.init().then(function () {
            return Promise.all([
                _producer.send({
                    topic: 'kafka-producer-topic-1',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }, { batch: { size: 10 } }),
                _producer.send({
                    topic: 'kafka-producer-topic-1',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }, { batch: { size: 10 } })
            ]);
        })
        .then(function () {
            spy.should.have.been.calledOnce; // eslint-disable-line
        });
    });

    it('should not group messages with different options', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer',
            batch: {
                size: 1
            }
        });

        var spy = sinon.spy(_producer.client, 'produceRequest');

        return _producer.init().then(function () {
            return Promise.all([
                _producer.send({
                    topic: 'kafka-producer-topic-1',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }, { batch: { size: 100 } }),
                _producer.send({
                    topic: 'kafka-producer-topic-1',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }, { batch: { size: 200 } })
            ]);
        })
        .then(function () {
            spy.should.have.been.calledTwice; // eslint-disable-line
        });
    });

    it('should wait up to maxWait time', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer',
            batch: {
                size: 16384,
                maxWait: 20
            }
        });

        var spy = sinon.spy(_producer.client, 'produceRequest');

        return _producer.init().then(function () {
            return _producer.send({
                topic: 'kafka-producer-topic-1',
                partition: 0,
                message: {
                    value: '12345'
                }
            })
            .delay(100)
            .then(function () {
                return _producer.send({
                    topic: 'kafka-producer-topic-1',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                });
            });
        })
        .then(function () {
            spy.should.have.been.calledTwice; // eslint-disable-line
        });
    });

    it('should retry on send failure', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer2'
        });

        var stub = sinon.stub(_producer.client, 'produceRequest');

        stub.onCall(0).resolves([{
            error: {
                code: 'UnknownTopicOrPartition'
            },
            topic: 'kafka-producer-topic-1',
            partition: 0,
        }]);
        stub.onCall(1).resolves({});

        return _producer.init().then(function () {
            return _producer.send({
                topic: 'kafka-producer-topic-1',
                partition: 0,
                message: {
                    value: '12345'
                }
            })
            .delay(200)
            .then(function () {
                stub.should.have.been.calledTwice; // eslint-disable-line
            });
        });
    });
});
