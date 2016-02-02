'use strict';

var Promise = require('bluebird');
var _       = require('lodash');
var Client  = require('./client');
var events  = require('events');
var util    = require('util');
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

    events.EventEmitter.call(this);
}

module.exports = BaseConsumer;

util.inherits(BaseConsumer, events.EventEmitter);

/**
 * Initialize BaseConsumer
 *
 * @return {Prommise}
 */
BaseConsumer.prototype.init = function () {
    return this.client.init();
};

BaseConsumer.prototype._fetch = function () {
    var self = this, immediate = false, data;

    if (self._fetch_running) {
        return;
    }

    data = _(self.subscriptions).values().groupBy('leader').mapValues(function (v) {
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
        self._fetch_running = false;
        return;
    }

    self._fetch_running = self.client.fetchRequest(data).then(function (results) {
        if (self._finished) { return null; }
        return Promise.map(results, function (p) {
            var s = self.subscriptions[p.topic + ':' + p.partition];
            if (!s) {
                return null; // already unsubscribed while we were polling
            }
            if (p.error) {
                if (p.error.code === 'OffsetOutOfRange') { // update partition offset to latest
                    self.client.warn('Updating offset because of OffsetOutOfRange error for', p.topic + ':' + p.partition);
                    return self._offset(s.leader, p.topic, p.partition, null, self.options.recoveryOffset).then(function (offset) {
                        s.offset = offset;
                    });
                } else if (/UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable/.test(p.error.code)) {
                    self.client.debug('Received', p.error.code, 'error for', p.topic + ':' + p.partition);
                    return self._updateSubscription(p.topic, p.partition);
                } else if (p.error instanceof errors.NoKafkaConnectionError) {
                    self.client.debug('Received connection error for', p.topic + ':' + p.partition);
                    return self._updateSubscription(p.topic, p.partition);
                }
                self.client.error(p.topic + ':' + p.partition, p.error);
            } else if (p.messageSet.length) {
                s.offset = _.last(p.messageSet).offset + 1; // advance offset position
                if (s.offset < p.highwaterMarkOffset) { // more messages available
                    immediate = true;
                }
                self.emit('data', p.messageSet, p.topic, p.partition);
            }
            return null;
        });
    })
    .catch(function (err) {
        if (self._finished) { return; }
        self.client.error(err);
    })
    .tap(function () {
        var schedule;
        if (self._finished) { return; }
        schedule = immediate ? process.nextTick : _.partialRight(setTimeout, self.options.idleTimeout);
        schedule(function () {
            self._fetch_running = false;
            self._fetch();
        });
    });

    return;
};

BaseConsumer.prototype._updateSubscription = function (topic, partition) {
    var self = this;

    return self.client.updateMetadata().then(function () {
        var s = self.subscriptions[topic + ':' + partition];

        return self.subscribe(topic, partition, s.offset ? { offset: s.offset } : s.options)
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
 * @param  {Number} partition
 * @param  {Object} options { maxBytes, offset, time }
 * @return {Promise}
 */
BaseConsumer.prototype.subscribe = function (topic, partition, options) {
    var self = this;

    partition = partition || 0;
    options = options || {};

    function _subscribe(leader, _offset) {
        self.client.debug('Subscribed to', topic + ':' + partition);
        self.subscriptions[topic + ':' + partition] = {
            topic: topic,
            partition: partition,
            offset: _offset,
            leader: leader,
            maxBytes: options.maxBytes || self.options.maxBytes
        };
        return _offset;
    }

    return self.client.findLeader(topic, partition).then(function (leader) {
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
            leader: -1
        };
    })
    .tap(function () {
        process.nextTick(function () {
            self._fetch();
        });
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

    return new Promise(function (resolve, reject) {
        if (self.subscriptions[topic + ':' + partition]) {
            delete self.subscriptions[topic + ':' + partition];
            return resolve();
        }
        reject(new Error('Not subscribed to', topic + ':' + partition));
    });
};
