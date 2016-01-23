'use strict';

var Promise = require('bluebird');
var _       = require('lodash');
var Client  = require('./client');
var errors  = require('./errors');

function Producer(options) {
    this.options = _.defaultsDeep(options || {}, {
        requiredAcks: 1,
        timeout: 100
    });

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
        return self.client.findLeader(d.topic, d.partition, true).then(function (leader) {
            d.leader = leader;
        });
    }, { concurrency: 10 })
    .then(function () {
        return _(data).groupBy('leader').mapValues(function (v) {
            return _(v)
                .groupBy('topic')
                .map(function (p, t) {
                    return {
                        topicName: t,
                        partitions: _(p).groupBy('partition').map(function (pv, pk) {
                            return {
                                partition: parseInt(pk),
                                messageSet: _.map(pv, function (mv) {
                                    return { offset: 0, message: mv.message };
                                })
                            };
                        }).value()
                    };
                })
                .value();
        }).value();
    });
};

/**
 * Send message or messages to Kafka
 *
 * @param  {Object|Array} data [{ topic, partition, message: {key, value, attributes} }]
 * @return {Promise}
 */
Producer.prototype.send = function (data) {
    var self = this, result = [];

    if (!Array.isArray(data)) {
        data = [data];
    }

    function _try(_data, attempt) {
        attempt = attempt || 0;

        return self._prepareProduceRequest(_data).then(function (requests) {
            return self.client.produceRequest(requests).then(function (response) {
                var toRetry = [];
                if (_.isEmpty(response)) { // if requiredAcks = 0
                    return response;
                }
                return Promise.map(response, function (p) {
                    if (p.error) {
                        if ((/UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable/.test(p.error.code)
                                || p.error instanceof errors.NoKafkaConnectionError)
                            && attempt < 3) {
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
                        return Promise.delay(attempt * 1000).then(function () {
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
