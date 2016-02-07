'use strict';

var Promise = require('bluebird');
var _       = require('lodash');
var Client  = require('./client');
var Kafka   = require('./index');
var errors  = require('./errors');

function BaseConsumer(options) {
    this.options = _.defaultsDeep(options || {}, {
        timeout: 100, // client timeout for produce and fetch requests
        idleTimeout: 1000, // timeout between fetch requests
        minBytes: 1,
        maxBytes: 1024 * 1024,
        recoveryOffset: Kafka.LATEST_OFFSET
    });

    this.client = new Client(this.options);

    this.subscriptions = {};

    this._finished = false;
}

module.exports = BaseConsumer;

/**
 * Initialize BaseConsumer
 *
 * @return {Prommise}
 */
BaseConsumer.prototype.init = function () {
    this._fetchPromise = this._fetch();
    return this.client.init();
};

BaseConsumer.prototype._fetch = function () {
    var self = this;

    return Promise.try(function () {
        var data = _(self.subscriptions).reject({ paused: true }).values().groupBy('leader').mapValues(function (v) {
            return _(v)
                .groupBy('topic')
                .map(function (p, t) {
                    return {
                        topicName: t,
                        partitions: p
                    };
                })
                .value();
        }).value();

        if (_.isEmpty(data)) {
            return null;
        }

        return self.client.fetchRequest(data).then(function (results) {
            if (self._finished) { return null; }
            return Promise.map(results, function (p) {
                var s = self.subscriptions[p.topic + ':' + p.partition];
                if (!s) {
                    return null; // already unsubscribed while we were polling
                }
                if (p.error) {
                    return self._partitionError(p.error, p.topic, p.partition);
                } else if (p.messageSet.length) {
                    s.paused = true;
                    s.handler(p.messageSet, p.topic, p.partition, p.highwaterMarkOffset)
                    .catch(function (err) {
                        self.client.warn('Handler for', p.topic + ':' + p.partition, 'failed with', err);
                    })
                    .finally(function () {
                        s.paused = false;
                        s.offset = _.last(p.messageSet).offset + 1; // advance offset position
                    });
                }
                return null;
            });
        });
    })
    .catch(function (err) {
        if (!self._finished) {
            self.client.error(err);
        }
    })
    .then(function () {
        if (!self._finished) {
            return Promise.delay(self.options.idleTimeout).then(function () {
                if (!self._finished) {
                    return self._fetch();
                }
            });
        }
    });
};

BaseConsumer.prototype._partitionError = function (err, topic, partition) {
    var self = this;

    var s = self.subscriptions[topic + ':' + partition];

    if (err.code === 'OffsetOutOfRange') { // update partition offset to options.recoveryOffset
        self.client.warn('Updating offset because of OffsetOutOfRange error for', topic + ':' + partition);
        return self._offset(s.leader, topic, partition, null, self.options.recoveryOffset).then(function (offset) {
            s.offset = offset;
        });
    } else if (/UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable/.test(err.code)) {
        self.client.debug('Received', err.code, 'error for', topic + ':' + partition);
        return self._updateSubscription(topic, partition);
    } else if (err instanceof errors.NoKafkaConnectionError) {
        self.client.debug('Received connection error for', topic + ':' + partition);
        return self._updateSubscription(topic, partition);
    }
    self.client.error(topic + ':' + partition, err);
};

BaseConsumer.prototype._updateSubscription = function (topic, partition) {
    var self = this;

    return self.client.updateMetadata().then(function () {
        var s = self.subscriptions[topic + ':' + partition];

        return self.subscribe(topic, partition, s.offset ? { offset: s.offset } : s.options, s.handler)
        .catch({ code: 'LeaderNotAvailable' }, function () {}) // ignore and try again on next fetch
        .catch(function (err) {
            self.client.error('Failed to re-subscribe to', topic + ':' + partition, err);
        });
    });
};

BaseConsumer.prototype._offset = function (leader, topic, partition, time) {
    var self = this, request = {};

    request[leader] = [{
        topicName: topic,
        partitions: [{
            partition: partition,
            time: time || Kafka.LATEST_OFFSET, // the latest (next) offset by default
            maxNumberOfOffsets: 1
        }]
    }];

    return self.client.offsetRequest(request).then(function (result) {
        var p = result[0];
        if (p.error) {
            throw p.error;
        }
        return p.offset[0];
    });
};

/**
 * Get offset for specified topic/partition and time
 *
 * @param  {String} topic
 * @param  {Number} partition
 * @param  {Number} time      Time in ms or two specific values: Kafka.EARLIEST_OFFSET and Kafka.LATEST_OFFSET
 * @return {Promise}
 */
BaseConsumer.prototype.offset = function (topic, partition, time) {
    var self = this;

    partition = partition || 0;

    return self.client.findLeader(topic, partition).then(function (leader) {
        return self._offset(leader, topic, partition, time);
    });
};


/**
 * Subscribe to topic/partition
 * @param  {String} topic
 * @param  {Number} [partition]
 * @param  {Object} [options] { maxBytes, offset, time }
 * @param  {Function} handler
 * @return {Promise}
 */
BaseConsumer.prototype.subscribe = function (topic, partition, options, handler) {
    var self = this;

    if (typeof partition === 'function') {
        handler = partition;
        partition = 0;
        options = {};
    } else if (typeof options === 'function') {
        handler = options;
        options = {};
    }

    function _subscribe(leader, _offset) {
        self.client.debug('Subscribed to', topic + ':' + partition);
        self.subscriptions[topic + ':' + partition] = {
            topic: topic,
            partition: partition,
            offset: _offset,
            leader: leader,
            maxBytes: options.maxBytes || self.options.maxBytes,
            handler: handler
        };
        return _offset;
    }

    return self.client.findLeader(topic, partition).then(function (leader) {
        handler = Promise.method(handler);

        if (options.offset >= 0) {
            return _subscribe(leader, options.offset);
        }
        return self._offset(leader, topic, partition, options.time).then(function (offset) {
            return _subscribe(leader, offset);
        });
    })
    .catch({ code: 'LeaderNotAvailable' }, function () {
        // these subscriptions will be retried on each _fetch()
        self.subscriptions[topic + ':' + partition] = {
            topic: topic,
            partition: partition,
            options: options,
            leader: -1,
            handler: handler
        };
    });
};

/**
 * Unsubscribe from topic/partition
 *
 * @param  {String} topic
 * @param  {Number} partition
 * @return {Promise}
 */
BaseConsumer.prototype.unsubscribe = function (topic, partition) {
    var self = this;

    partition = partition || 0;

    return new Promise(function (resolve) {
        delete self.subscriptions[topic + ':' + partition];
        return resolve();
    });
};

BaseConsumer.prototype.end = function () {
    var self = this;

    self._finished = true;
    self.subscriptions = {};

    return self._fetchPromise.then(function () {
        return self.client.end();
    });
};
