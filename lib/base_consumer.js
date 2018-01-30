'use strict';

var Promise = require('./bluebird-configured');
var _       = require('lodash');
var Client  = require('./client');
var Kafka   = require('./index');
var Fetcher = require('./fetcher');

function BaseConsumer(options) {
    this.options = _.defaultsDeep(options || {}, {
        maxWaitTime: 100,
        idleTimeout: 1000, // timeout between fetch requests
        minBytes: 1,
        maxBytes: 1024 * 1024,
        recoveryOffset: Kafka.LATEST_OFFSET,
        handlerConcurrency: 10
    });

    this.client = new Client(this.options);

    this.subscriptions = {};

    this.fetcher = new Fetcher(this);
}

module.exports = BaseConsumer;

/**
 * Initialize BaseConsumer
 *
 * @return {Prommise}
 */
BaseConsumer.prototype.init = function () {
    this.fetcher.start();
    return this.client.init();
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
        return self.client.getPartitionOffset(leader, topic, partition, time);
    });
};


/**
 * Subscribe to topic/partition
 * @param  {String} topic
 * @param  {Array} [partitions]
 * @param  {Object} [options] { maxBytes, offset, time }
 * @param  {Function} handler
 * @return {Promise}
 */
BaseConsumer.prototype.subscribe = function (topic, partitions, options, handler) {
    var self = this;

    if (typeof partitions === 'function') {
        handler = partitions;
        partitions = [];
        options = {};
    } else if (typeof options === 'function') {
        handler = options;
        if (_.isPlainObject(partitions)) {
            options = partitions;
            partitions = [];
        } else {
            options = {};
        }
    }

    if (typeof handler !== 'function') {
        return Promise.reject(new Error('Missing data handler for subscription'));
    }

    if (!Array.isArray(partitions)) {
        partitions = [partitions];
    }

    if (partitions.length === 0) {
        partitions = self.client.getTopicPartitions(topic).then(_.partialRight(_.map, 'partitionId'));
    }

    function _subscribe(partition, leader, offset) {
        self.client.debug('Subscribed to', topic + ':' + partition, 'offset', offset, 'leader', self.client.leaderServer(leader));
        self.subscriptions[topic + ':' + partition] = {
            topic: topic,
            partition: partition,
            offset: offset,
            leader: leader,
            maxBytes: options.maxBytes || self.options.maxBytes,
            handler: handler
        };
        return offset;
    }

    return Promise.map(partitions, function (partition) {
        return self.client.findLeader(topic, partition).then(function (leader) {
            handler = Promise.method(handler);

            if (options.offset >= 0) {
                return _subscribe(partition, leader, options.offset);
            }
            return self.client.getPartitionOffset(leader, topic, partition, options.time).then(function (offset) {
                return _subscribe(partition, leader, offset);
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
    });
};

/**
 * Unsubscribe from topic/partition
 *
 * @param  {String} topic
 * @param  {Array} partitions
 * @return {Promise}
 */
BaseConsumer.prototype.unsubscribe = function (topic, partitions) {
    var self = this;

    if (partitions === undefined) {
        partitions = [];
    }

    if (!Array.isArray(partitions)) {
        partitions = [partitions];
    }

    if (partitions.length === 0) {
        partitions = Object.keys(self.client.topicMetadata[topic]).map(_.ary(parseInt, 1));
    }

    return Promise.map(partitions, function (partition) {
        delete self.subscriptions[topic + ':' + partition];
    });
};

BaseConsumer.prototype.end = function () {
    var self = this;

    self.subscriptions = {};

    return self.fetcher.stop().then(function () {
        return self.client.end();
    });
};

// vim: set sw=4 ts=4 expandtab:

