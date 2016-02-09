'use strict';

var Promise = require('bluebird');
var _       = require('lodash');
var Client  = require('./client');
var Kafka   = require('./index');
var errors  = require('./errors');

function Producer(options) {
    this.options = _.defaultsDeep(options || {}, {
        requiredAcks: 1,
        timeout: 100,
        retries: {
            attempts: 3,
            delay: 1000
        },
        codec: Kafka.COMPRESSION_NONE
    });

    if (options.hasOwnProperty('partitioner') && typeof options.partitioner !== 'function') {
        throw new Error('Partitioner must be a function');
    }

    this.partitioner = options.partitioner;
    this.client = new Client(this.options);
}

module.exports = Producer;

/**
 * Initialize Producer
 *
 * @return {Promise}
 */
Producer.prototype.init = function () {
    return this.client.init();
};

Producer.prototype._prepareProduceRequest = function (data) {
    var self = this;

    return Promise.map(data, function (d) {
        if (typeof d.topic !== 'string' || d.topic === '') {
            throw new Error('Missing or wrong topic field');
        }
        if (self.partitioner && !d.hasOwnProperty('partition')) {
            d.partition = self.partitioner(d.topic, Object.keys(self.client.topicMetadata[d.topic]), d.message);
        }
        if (typeof d.partition !== 'number' || d.partition < 0) {
            throw new Error('Missing or wrong partition field');
        }

        return self.client.findLeader(d.topic, d.partition, true).then(function (leader) {
            d.leader = leader;
        });
    }, { concurrency: 10 }).return(data);
};

/**
 * Send message or messages to Kafka
 *
 * @param  {Object|Array} data [{ topic, partition, message: {key, value, attributes} }]
 * @param  {Object} options { codec, retries: { attempts, delay } }
 * @return {Promise}
 */
Producer.prototype.send = function (data, options) {
    var self = this, result = [];

    if (!Array.isArray(data)) {
        data = [data];
    }

    options = _.merge({}, {
        codec: self.options.codec,
        retries: self.options.retries
    }, options || {});

    function _try(_data, attempt) {
        attempt = attempt || 1;

        return self._prepareProduceRequest(_data).then(function (requests) {
            return self.client.produceRequest(requests, options.codec).then(function (response) {
                var toRetry = [];
                if (_.isEmpty(response)) { // if requiredAcks = 0
                    return response;
                }
                return Promise.map(response, function (p) {
                    if (p.error) {
                        if ((/UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable/.test(p.error.code)
                                || p.error instanceof errors.NoKafkaConnectionError)
                            && attempt < options.retries.attempts) {
                            // self.client.debug('Received', p.error, 'for', p.topic + ':' + p.partition);
                            toRetry = toRetry.concat(_.filter(_data, { topic: p.topic, partition: p.partition }));
                        } else {
                            result.push(p);
                        }
                    } else {
                        result.push(p);
                    }
                })
                .then(function () {
                    if (toRetry.length) {
                        return Promise.delay(options.retries.delay).then(function () {
                            return self.client.updateMetadata().then(function () {
                                return _try(toRetry, ++attempt);
                            });
                        });
                    }
                }).return(result);
            });
        });
    }

    return _try(data);
};

/**
 * Close all connections
 *
 * @return {Promise}
 */
Producer.prototype.end = function () {
    var self = this;

    return self.client.end();
};
