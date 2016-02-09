'use strict';

/* global describe, it, before, sinon, after  */

var crc32   = require('buffer-crc32');
var Promise = require('bluebird');
var Kafka   = require('../lib/index');

describe('Compression', function () {
    var producer = new Kafka.Producer({ clientId: 'producer' });
    var consumer = new Kafka.SimpleConsumer({ idleTimeout: 100, clientId: 'simple-consumer' });

    var dataHandlerSpy = sinon.spy(function () {});

    before(function () {
        return Promise.all([
            producer.init(),
            consumer.init()
        ])
        .then(function () {
            consumer.subscribe('kafka-test-topic', 0, dataHandlerSpy);
        });
    });

    after(function () {
        return Promise.all([
            producer.end(),
            consumer.end()
        ]);
    });

    it('should send/receive with Snappy compression (<32kb)', function () {
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'p00' }
        }, { codec: Kafka.COMPRESSION_SNAPPY })
        .delay(100)
        .then(function () {
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHandlerSpy.lastCall.args[1].should.be.a('string', 'kafka-test-topic');
            dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

            dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
            dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
            dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });

    it('should send/receive with Snappy compression (>32kb)', function () {
        var buf = new Buffer(90 * 1024), crc = crc32.signed(buf);

        dataHandlerSpy.reset();

        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: buf }
        }, { codec: Kafka.COMPRESSION_SNAPPY })
        .delay(300)
        .then(function () {
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHandlerSpy.lastCall.args[1].should.be.a('string', 'kafka-test-topic');
            dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

            dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
            dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
            crc32.signed(dataHandlerSpy.lastCall.args[0][0].message.value).should.be.eql(crc);
        });
    });

    if (typeof require('zlib').gzipSync === 'function') {
        it('should send/receive with gzip compression', function () {
            return producer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p00' }
            }, { codec: Kafka.COMPRESSION_GZIP })
            .delay(100)
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpy.lastCall.args[1].should.be.a('string', 'kafka-test-topic');
                dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

                dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
                dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
                dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
                dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
            });
        });
    }
});
