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

describe('null and empty', function () {
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

    it('should send/receive null byte string', function () {
        return consumer.subscribe('kafka-test-topic', 0, dataHanlderSpy).then(function () {
            return producer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: null, key: null }
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
            dataHanlderSpy.lastCall.args[0][0].message.should.have.property('value', null);
            dataHanlderSpy.lastCall.args[0][0].message.should.have.property('key', null);
        });
    });

    it('should send/receive empty byte string', function () {
        dataHanlderSpy.reset();
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: '', key: '' }
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
            dataHanlderSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('');
            dataHanlderSpy.lastCall.args[0][0].message.should.have.property('key');
            dataHanlderSpy.lastCall.args[0][0].message.key.toString('utf8').should.be.eql('');
        });
    });
});

describe('connectionString', function () {
    it('should throw when connectionString is wrong', function () {
        var producer = new Kafka.Producer({ connectionString: 'localhost' });

        return producer.init().should.be.rejected;
    });
});

describe('brokerRedirection', function () {
    it('Should execute a function passed for direction', function () {
        var latched = false;
        var producer = new Kafka.Producer({
            brokerRedirection: function (host, port) {
                latched = true;
                return {
                    host: host,
                    port: port
                };
            }
        });

        return producer.init()
            .then(function () {
                latched.should.eql(true);
            });
    });

    it('Should apply function remapping', function () {
        var latched = false;
        var producer = new Kafka.Producer({
            connectionString: 'does-not-exist:9092',
            ssl: false,
            brokerRedirection: function () {
                latched = true;
                return {
                    host: 'localhost',
                    port: 9092
                };
            }
        });

        // If the init is succesful, then we remapped the bad
        // broker name.
        return producer.init()
            .then(function () {
                latched.should.eql(true);
            });
    });

    it('Should apply lookup remap (host:port)', function () {
        var producer = new Kafka.Producer({
            connectionString: 'does-not-exist:9092',
            ssl: false,
            brokerRedirection: {
                'does-not-exist:9092': 'localhost:9092'
            }
        });

        // If the init is succesful, then we remapped the bad
        // broker name.
        return producer.init();
    });

    it('Should apply lookup remap (kafka://host:port)', function () {
        var producer = new Kafka.Producer({
            connectionString: 'does-not-exist:9092',
            ssl: false,
            brokerRedirection: {
                'kafka://does-not-exist:9092': 'localhost:9092'
            }
        });

        // If the init is succesful, then we remapped the bad
        // broker name.
        return producer.init();
    });

    it('Should apply lookup remap prefixed with Kafka', function () {
        var producer = new Kafka.Producer({
            connectionString: 'does-not-exist:9092',
            ssl: false,
            brokerRedirection: {
                'kafka://does-not-exist:9092': 'kafka://localhost:9092'
            }
        });

        // If the init is succesful, then we remapped the bad
        // broker name.
        return producer.init();
    });
});
