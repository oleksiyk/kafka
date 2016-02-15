'use strict';

/* global describe, it, before, sinon, after  */

var Promise = require('bluebird');
var Kafka   = require('../lib/index');

describe('requiredAcks: 0', function () {
    var producer = new Kafka.Producer({ requiredAcks: 0, clientId: 'producer' });
    var consumer = new Kafka.SimpleConsumer({ idleTimeout: 100, clientId: 'simple-consumer' });

    var dataHanlderSpy = sinon.spy(function () {});

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

    it('should send/receive messages', function () {
        return consumer.subscribe('kafka-test-topic', 0, dataHanlderSpy).then(function () {
            return producer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p00' }
            });
        })
        .delay(100)
        .then(function () {
            dataHanlderSpy.should.have.been.called; // eslint-disable-line
            dataHanlderSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHanlderSpy.lastCall.args[1].should.be.a('string', 'kafka-test-topic');
            dataHanlderSpy.lastCall.args[2].should.be.a('number', 0);

            dataHanlderSpy.lastCall.args[0][0].should.be.an('object');
            dataHanlderSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHanlderSpy.lastCall.args[0][0].message.should.have.property('value');
            dataHanlderSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });
});

describe('connectionString', function () {
    it('should throw when connectionString is wrong', function () {
        var producer = new Kafka.Producer({ connectionString: 'localhost' });

        return producer.init().should.be.rejected;
    });
});
