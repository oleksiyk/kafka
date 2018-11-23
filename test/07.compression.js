'use strict';

/* global describe, it, before, sinon, after  */

var crc32   = require('buffer-crc32');
var Promise = require('bluebird');
var Kafka   = require('../lib/index');
var { getConnectionString, createTopics } = require('./testkit/kafka');

describe('Compression', function () {
    describe('sync', function () {
        var KAFKA_TOPIC = 'kafka-compression-sync-test-topic';
        var producer;
        var consumer;

        var dataHandlerSpy = sinon.spy(function () {});

        before(function () {
            producer = new Kafka.Producer({
                clientId: 'producer',
                asyncCompression: false,
                connectionString: getConnectionString(),
            });
            consumer = new Kafka.SimpleConsumer({
                idleTimeout: 100,
                clientId: 'simple-consumer',
                asyncCompression: false,
                connectionString: getConnectionString(),
            });
            return createTopics([KAFKA_TOPIC])
            .then(function () {
                return Promise.all([
                    producer.init(),
                    consumer.init()
                ]);
            })
            .then(function () {
                return consumer.subscribe(KAFKA_TOPIC, 0, dataHandlerSpy);
            });
        });

        after(function () {
            return Promise.all([
                producer.end(),
                consumer.end()
            ]);
        });

        it('should send/receive with Snappy compression (<32kb)', function () {
            var offset;
            return Promise.all([
                producer.send({
                    topic: KAFKA_TOPIC,
                    partition: 0,
                    message: { value: 'p00' }
                }, { codec: Kafka.COMPRESSION_SNAPPY }),
                producer.send({
                    topic: KAFKA_TOPIC,
                    partition: 0,
                    message: { value: 'p01' }
                }, { codec: Kafka.COMPRESSION_SNAPPY }),
                Promise.delay(20).then(function () {
                    return producer.send({
                        topic: KAFKA_TOPIC,
                        partition: 0,
                        message: { value: 'p02' }
                    }, { codec: Kafka.COMPRESSION_SNAPPY });
                })
            ])
            .then(function () {
                return consumer.offset(KAFKA_TOPIC, 0, Kafka.LATEST_OFFSET).then(function (_offset) {
                    offset = _offset - 3;
                });
            })
            .then(function () {
                return consumer.subscribe(KAFKA_TOPIC, 0, { offset: offset }, dataHandlerSpy);
            })
            .delay(300)
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(3);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
                dataHandlerSpy.lastCall.args[0][1].message.value.toString('utf8').should.be.eql('p01');
                dataHandlerSpy.lastCall.args[0][2].message.value.toString('utf8').should.be.eql('p02');
            });
        });

        it('should send/receive with Snappy compression (>32kb)', function () {
            var buf = new Buffer(90 * 1024), crc = crc32.signed(buf);

            dataHandlerSpy.reset();

            return producer.send({
                topic: KAFKA_TOPIC,
                partition: 0,
                message: { value: buf }
            }, { codec: Kafka.COMPRESSION_SNAPPY })
            .delay(300)
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                crc32.signed(dataHandlerSpy.lastCall.args[0][0].message.value).should.be.eql(crc);
            });
        });

        if (typeof require('zlib').gzipSync === 'function') {
            it('should send/receive with gzip compression', function () {
                dataHandlerSpy.reset();
                return producer.send({
                    topic: KAFKA_TOPIC,
                    partition: 0,
                    message: { value: 'p00' }
                }, { codec: Kafka.COMPRESSION_GZIP })
                .delay(200)
                .then(function () {
                    dataHandlerSpy.should.have.been.called; // eslint-disable-line
                    dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                    dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
                    dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                    dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                    dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                    dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                    dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
                });
            });
        }

        it('producer should send uncompressed message when codec is not supported', function () {
            dataHandlerSpy.reset();
            return producer.send({
                topic: KAFKA_TOPIC,
                partition: 0,
                message: { value: 'p00' }
            }, { codec: 30 })
            .delay(200)
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
            });
        });
    });

    describe('async', function () {
        var KAFKA_TOPIC = 'kafka-compression-async-test-topic';
        var producer;
        var consumer;

        var dataHandlerSpy = sinon.spy(function () {});

        before(function () {
            producer = new Kafka.Producer({
                clientId: 'producer',
                asyncCompression: true,
                connectionString: getConnectionString(),
            });
            consumer = new Kafka.SimpleConsumer({
                idleTimeout: 100,
                clientId: 'simple-consumer',
                asyncCompression: true,
                connectionString: getConnectionString(),
            });

            return createTopics([
                KAFKA_TOPIC
            ]).then(function () {
                return Promise.all([
                    producer.init(),
                    consumer.init()
                ]);
            })
            .then(function () {
                return consumer.subscribe(KAFKA_TOPIC, 0, dataHandlerSpy);
            });
        });

        after(function () {
            return Promise.all([
                producer.end(),
                consumer.end()
            ]);
        });

        it('should send/receive with async Snappy compression (<32kb)', function () {
            var offset;
            return Promise.all([
                producer.send({
                    topic: KAFKA_TOPIC,
                    partition: 0,
                    message: { value: 'p00' }
                }, { codec: Kafka.COMPRESSION_SNAPPY }),
                producer.send({
                    topic: KAFKA_TOPIC,
                    partition: 0,
                    message: { value: 'p01' }
                }, { codec: Kafka.COMPRESSION_SNAPPY }),
                Promise.delay(20).then(function () {
                    return producer.send({
                        topic: KAFKA_TOPIC,
                        partition: 0,
                        message: { value: 'p02' }
                    }, { codec: Kafka.COMPRESSION_SNAPPY });
                })
            ])
            .then(function () {
                return consumer.offset(KAFKA_TOPIC, 0, Kafka.LATEST_OFFSET).then(function (_offset) {
                    offset = _offset - 3;
                });
            })
            .then(function () {
                dataHandlerSpy.reset();
                return consumer.subscribe(KAFKA_TOPIC, 0, { offset: offset }, dataHandlerSpy);
            })
            .delay(200)
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(3);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
                dataHandlerSpy.lastCall.args[0][1].message.value.toString('utf8').should.be.eql('p01');
                dataHandlerSpy.lastCall.args[0][2].message.value.toString('utf8').should.be.eql('p02');
            });
        });

        it('should send/receive with async Snappy compression (>32kb)', function () {
            var buf = new Buffer(90 * 1024), crc = crc32.signed(buf);

            dataHandlerSpy.reset();

            return producer.send({
                topic: KAFKA_TOPIC,
                partition: 0,
                message: { value: buf }
            }, { codec: Kafka.COMPRESSION_SNAPPY })
            .delay(300)
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                crc32.signed(dataHandlerSpy.lastCall.args[0][0].message.value).should.be.eql(crc);
            });
        });

        it('should send/receive with async gzip compression', function () {
            dataHandlerSpy.reset();

            return producer.send({
                topic: KAFKA_TOPIC,
                partition: 0,
                message: { value: 'p00' }
            }, { codec: Kafka.COMPRESSION_GZIP })
            .delay(200)
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
            });
        });

        it('should send/receive with async Gzip compression (>32kb)', function () {
            var buf = new Buffer(90 * 1024), crc = crc32.signed(buf);

            dataHandlerSpy.reset();

            return producer.send({
                topic: KAFKA_TOPIC,
                partition: 0,
                message: { value: buf }
            }, { codec: Kafka.COMPRESSION_GZIP })
            .delay(300)
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                crc32.signed(dataHandlerSpy.lastCall.args[0][0].message.value).should.be.eql(crc);
            });
        });

        it('producer should send uncompressed message when codec is not supported', function () {
            dataHandlerSpy.reset();
            return producer.send({
                topic: KAFKA_TOPIC,
                partition: 0,
                message: { value: 'p00' }
            }, { codec: 30 })
            .delay(200)
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
            });
        });
    });
});
