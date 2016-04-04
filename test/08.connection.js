'use strict';

/* global describe, it, before, sinon, after  */

var Promise = require('bluebird');
var crc32   = require('buffer-crc32');
var Kafka   = require('../lib/index');

describe('Connection', function () {
    var producer = new Kafka.Producer({ requiredAcks: 0, clientId: 'producer' });
    var consumer = new Kafka.SimpleConsumer({ idleTimeout: 100, clientId: 'simple-consumer' });

    var dataHandlerSpy = sinon.spy(function () {});

    before(function () {
        return Promise.all([
            producer.init(),
            consumer.init()
        ])
        .then(function () {
            return consumer.subscribe('kafka-test-topic', 0, dataHandlerSpy);
        });
    });

    after(function () {
        return Promise.all([
            producer.end(),
            consumer.end()
        ]);
    });

    it('should be able to grow receive buffer', function () {
        var buf = new Buffer(384 * 1024), crc = crc32.signed(buf);

        dataHandlerSpy.reset();

        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: buf }
        })
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

    it('should parse connection string with protocol', function () {
        var p = new Kafka.Producer({ connectionString: 'kafka://127.0.0.1:9092' });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(1);
            p.client.initialBrokers[0].host.should.be.eql('127.0.0.1');
            p.client.initialBrokers[0].port.should.be.eql(9092);
        });
    });

    it('should parse connection string without protocol', function () {
        var p = new Kafka.Producer({ connectionString: '127.0.0.1:9092' });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(1);
            p.client.initialBrokers[0].host.should.be.eql('127.0.0.1');
            p.client.initialBrokers[0].port.should.be.eql(9092);
        });
    });

    it('should parse connection string with multiple hosts with and without protocol', function () {
        var p = new Kafka.Producer({ connectionString: 'kafka://127.0.0.1:9092,127.0.0.1:9092' });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(2);
            p.client.initialBrokers[0].host.should.be.eql('127.0.0.1');
            p.client.initialBrokers[0].port.should.be.eql(9092);
            p.client.initialBrokers[1].host.should.be.eql('127.0.0.1');
            p.client.initialBrokers[1].port.should.be.eql(9092);
        });
    });

    it('should parse connection string with multiple hosts without protocol', function () {
        var p = new Kafka.Producer({ connectionString: '127.0.0.1:9092,127.0.0.1:9092' });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(2);
            p.client.initialBrokers[0].host.should.be.eql('127.0.0.1');
            p.client.initialBrokers[0].port.should.be.eql(9092);
            p.client.initialBrokers[1].host.should.be.eql('127.0.0.1');
            p.client.initialBrokers[1].port.should.be.eql(9092);
        });
    });

    it('should strip whitespaces in connectionString', function () {
        var p = new Kafka.Producer({ connectionString: ' kafka://127.0.0.1:9092, localhost:9092 ' });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(2);
            p.client.initialBrokers[0].host.should.be.eql('127.0.0.1');
            p.client.initialBrokers[0].port.should.be.eql(9092);
            p.client.initialBrokers[1].host.should.be.eql('localhost');
            p.client.initialBrokers[1].port.should.be.eql(9092);
        });
    });
});
