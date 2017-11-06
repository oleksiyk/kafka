'use strict';

/* global describe, it, before, sinon, after  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.9 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var Promise = require('bluebird');
var Kafka   = require('../lib/index');
var _       = require('lodash');

var producer = new Kafka.Producer({ requiredAcks: 1, clientId: 'producer' });
var consumers = [
    new Kafka.GroupConsumer({
        idleTimeout: 100,
        heartbeatTimeout: 100,
        clientId: 'group-consumer1'
    }),
    new Kafka.GroupConsumer({
        idleTimeout: 100,
        heartbeatTimeout: 100,
        clientId: 'group-consumer2'
    }),
    new Kafka.GroupConsumer({
        idleTimeout: 100,
        heartbeatTimeout: 100,
        clientId: 'group-consumer3'
    })
];
var dataHandlerSpies;

function dataHandlerFactory(consumer) {
    return sinon.spy(function (messageSet, topic, partition) {
        return Promise.each(messageSet, function (m) {
            return consumer.commitOffset({ topic: topic, partition: partition, offset: m.offset });
        });
    });
}

dataHandlerSpies = [
    dataHandlerFactory(consumers[0]),
    dataHandlerFactory(consumers[1]),
    dataHandlerFactory(consumers[2]),
];

describe('GroupConsumer', function () {
    before(function () {
        this.timeout(6000); // let Kafka create offset topic
        return Promise.all([
            producer.init(),
            consumers[0].init({
                subscriptions: ['kafka-test-topic'],
                handler: dataHandlerSpies[0]
            }).delay(200) // let it consume previous messages in a topic (if any)
        ]);
    });

    after(function () {
        return Promise.all([
            Promise.map(consumers, function (c) {
                return c.end();
            }),
            producer.end()
        ]);
    });

    it('required methods', function () {
        return consumers[0].should
            .respondTo('init')
            .respondTo('subscribe')
            .respondTo('offset')
            .respondTo('unsubscribe')
            .respondTo('commitOffset')
            .respondTo('fetchOffset')
            .respondTo('end');
    });

    it('should receive new messages', function () {
        dataHandlerSpies[0].reset();
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'p00' }
        })
        .delay(200)
        .then(function () {
            /* jshint expr: true */
            dataHandlerSpies[0].should.have.been.called; //eslint-disable-line
            dataHandlerSpies[0].lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHandlerSpies[0].lastCall.args[1].should.be.a('string', 'kafka-test-topic');
            dataHandlerSpies[0].lastCall.args[2].should.be.a('number', 0);

            dataHandlerSpies[0].lastCall.args[0][0].should.be.an('object');
            dataHandlerSpies[0].lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHandlerSpies[0].lastCall.args[0][0].message.should.have.property('value');
            dataHandlerSpies[0].lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });

    it('should be able to commit offsets', function () {
        return consumers[0].commitOffset([
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
            }
        ]).then(function (result) {
            result.should.be.an('array').that.has.length(2);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[1].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('error', null);
            result[1].should.have.property('error', null);
        });
    });

    it('should be able to fetch commited offsets', function () {
        return consumers[0].fetchOffset([
            {
                topic: 'kafka-test-topic',
                partition: 0
            },
            {
                topic: 'kafka-test-topic',
                partition: 1
            }
        ]).then(function (result) {
            result.should.be.an('array').that.has.length(2);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[1].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('offset').that.is.a('number');
            result[1].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('metadata').that.is.a('string');
            result[1].should.have.property('metadata').that.is.a('string');
            result[0].should.have.property('error', null);
            result[1].should.have.property('error', null);
            _.find(result, { topic: 'kafka-test-topic', partition: 0 }).offset.should.be.eql(1 + 1);
            _.find(result, { topic: 'kafka-test-topic', partition: 1 }).offset.should.be.eql(2 + 1);
            _.find(result, { topic: 'kafka-test-topic', partition: 0 }).metadata.should.be.eql('m1');
            _.find(result, { topic: 'kafka-test-topic', partition: 1 }).metadata.should.be.eql('m2');
        });
    });

    it('offset() should return last offset', function () {
        return Promise.all([
            consumers[0].offset('kafka-test-topic', 0),
            consumers[0].offset('kafka-test-topic', 1),
            consumers[0].offset('kafka-test-topic', 2),
        ]).spread(function (offset0, offset1, offset2) {
            offset0.should.be.a('number').and.be.gt(0);
            offset1.should.be.a('number').and.be.gt(0);
            offset2.should.be.a('number').and.be.gt(0);

            // commit them back for next tests
            return Promise.all([
                consumers[0].commitOffset({ topic: 'kafka-test-topic', partition: 0, offset: offset0 - 1 }),
                consumers[0].commitOffset({ topic: 'kafka-test-topic', partition: 1, offset: offset1 - 1 }),
                consumers[0].commitOffset({ topic: 'kafka-test-topic', partition: 2, offset: offset2 - 1 })
            ]);
        });
    });

    it('should split partitions in a group', function () {
        this.timeout(6000);
        return Promise.all([
            consumers[1].init({
                subscriptions: ['kafka-test-topic'],
                handler: dataHandlerSpies[1]
            }),
            consumers[2].init({
                subscriptions: ['kafka-test-topic'],
                handler: dataHandlerSpies[2]
            }),
        ])
        .delay(1000) // give some time to rebalance group
        .then(function () {
            dataHandlerSpies[0].reset();
            return producer.send([
                {
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: { value: 'p00' }
                },
                {
                    topic: 'kafka-test-topic',
                    partition: 1,
                    message: { value: 'p01' }
                },
                {
                    topic: 'kafka-test-topic',
                    partition: 2,
                    message: { value: 'p02' }
                }
            ])
            .delay(400)
            .then(function () {
                /* jshint expr: true */
                dataHandlerSpies[0].should.have.been.calledOnce; //eslint-disable-line
                dataHandlerSpies[0].lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpies[1].should.have.been.calledOnce; //eslint-disable-line
                dataHandlerSpies[1].lastCall.args[0].should.be.an('array').and.have.length(1);
                dataHandlerSpies[2].should.have.been.calledOnce; //eslint-disable-line
                dataHandlerSpies[2].lastCall.args[0].should.be.an('array').and.have.length(1);
            });
        });
    });

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
