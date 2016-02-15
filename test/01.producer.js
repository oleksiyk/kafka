'use strict';

/* global describe, it, before, after, sinon  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.9 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var Promise = require('bluebird');
var Kafka   = require('../lib/index');

var producer = new Kafka.Producer({
    requiredAcks: 1,
    clientId: 'producer'
});

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

    it('should return an error for unknown partition/topic and retry 5 times', function () {
        var start = Date.now(), msgs;
        msgs = [{
            topic: 'kafka-test-unknown-topic',
            partition: 0,
            message: { value: 'Hello!' }
        }, {
            topic: 'kafka-test-topic',
            partition: 20,
            message: { value: 'Hello!' }
        }];
        return producer.send(msgs, {
            retries: {
                attempts: 5,
                delay: 100
            }
        }).then(function (result) {
            result.should.be.an('array').and.have.length(2);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[0].should.have.property('error');
            result[1].should.have.property('error');
            result[0].error.should.have.property('code', 'UnknownTopicOrPartition');
            result[1].error.should.have.property('code', 'UnknownTopicOrPartition');
            (Date.now() - start).should.be.closeTo(500, 100);
        });
    });

    it('partitioner arguments', function () {
        var partitionerSpy = sinon.spy(function () { return 1; });
        var _producer = new Kafka.Producer({
            clientId: 'producer',
            partitioner: partitionerSpy
        });
        return _producer.init().then(function () {
            return _producer.send({
                topic: 'kafka-test-topic',
                message: {
                    value: 'Hello!'
                }
            });
        })
        .then(function () {
            partitionerSpy.should.have.been.called; // eslint-disable-line
            partitionerSpy.lastCall.args[0].should.be.a('string').that.is.eql('kafka-test-topic');
            partitionerSpy.lastCall.args[1].should.be.an('array').and.have.length(3);
            partitionerSpy.lastCall.args[1][0].should.be.an('object');
            partitionerSpy.lastCall.args[1][0].should.have.property('partitionId').that.is.a('number');
            partitionerSpy.lastCall.args[1][0].should.have.property('error').that.is.eql(null);
            partitionerSpy.lastCall.args[1][0].should.have.property('leader').that.is.a('number');
            partitionerSpy.lastCall.args[1][0].should.have.property('replicas').that.is.an('array');
            partitionerSpy.lastCall.args[1][0].should.have.property('isr').that.is.an('array');
        });
    });

    it('should determine topic partition using sync partitioner function', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer',
            partitioner: function dummySyncPartitioner(/*topicName, partitions, message*/) {
                return 1;
            }
        });
        return _producer.init().then(function () {
            return _producer.send({
                topic: 'kafka-test-topic',
                message: {
                    value: 'Hello!'
                }
            });
        })
        .then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition', 1);
            result[0].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
        });
    });

    it('should determine topic partition using async partitioner function', function () {
        var _producer = new Kafka.Producer({
            clientId: 'producer',
            partitioner: function dummyASyncPartitioner(/*topicName, partitions, message*/) {
                return Promise.delay(100).then(function () {
                    return 2;
                });
            }
        });
        return _producer.init().then(function () {
            return _producer.send({
                topic: 'kafka-test-topic',
                message: {
                    value: 'Hello!'
                }
            });
        })
        .then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition', 2);
            result[0].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
        });
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
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }),
                _producer.send({
                    topic: 'kafka-test-topic',
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
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }),
                _producer.send({
                    topic: 'kafka-test-topic',
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
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }, { batch: { size: 10 } }),
                _producer.send({
                    topic: 'kafka-test-topic',
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
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        value: '12345'
                    }
                }, { batch: { size: 100 } }),
                _producer.send({
                    topic: 'kafka-test-topic',
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
                topic: 'kafka-test-topic',
                partition: 0,
                message: {
                    value: '12345'
                }
            })
            .delay(100)
            .then(function () {
                return _producer.send({
                    topic: 'kafka-test-topic',
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
});
