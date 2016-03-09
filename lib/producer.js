'use strict';

var Promise            = require('./bluebird-configured');
var _                  = require('lodash');
var Client             = require('./client');
var Kafka              = require('./index');
var errors             = require('./errors');
var DefaultPartitioner = require('./default_partitioner');

function Producer(options) {
    this.options = _.defaultsDeep(options || {}, {
        requiredAcks: 1,
        timeout: 100,
        partitioner: new DefaultPartitioner(),
        retries: {
            attempts: 3,
            delay: 1000
        },
        batch: {
            size: 16384,
            maxWait: 10
        },
        codec: Kafka.COMPRESSION_NONE
    });

    if (this.options.partitioner instanceof DefaultPartitioner) {
        this.partitioner = this.options.partitioner;
    } else {
        throw new Error('Partitioner must inherit from DefaultPartitioner');
    }

    this.client = new Client(this.options);

    this.queue = {};
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
        return Promise.try(function () {
            if (typeof d.topic !== 'string' || d.topic === '') {
                throw new Error('Missing or wrong topic field');
            }
            if (!d.hasOwnProperty('partition')) {
                return self.client.getTopicPartitions(d.topic).then(function (partitions) {
                    return self.partitioner.partition(d.topic, partitions, d.message);
                })
                .then(function (partition) {
                    d.partition = partition;
                });
            }
            return null;
        })
        .then(function () {
            if (typeof d.partition !== 'number' || d.partition < 0) {
                throw new Error('Missing or wrong partition field');
            }
            return self.client.findLeader(d.topic, d.partition, true).then(function (leader) {
                d.leader = leader;
            });
        });
    }, { concurrency: 10 }).return(data);
};

Producer.prototype._send = function (hash) {
    var self = this, task = self.queue[hash], data, result = [];

    delete self.queue[hash];

    data = Array.prototype.concat.apply([], task.data);

    (function _try(_data, attempt) {
        attempt = attempt || 1;

        return self._prepareProduceRequest(_data).then(function (requests) {
            return self.client.produceRequest(requests, task.options.codec).then(function (response) {
                var toRetry = [];
                if (_.isEmpty(response)) { // if requiredAcks = 0
                    return response;
                }
                return Promise.map(response, function (p) {
                    if (p.error) {
                        if ((/UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable/.test(p.error.code)
                                || p.error instanceof errors.NoKafkaConnectionError)
                            && attempt < task.options.retries.attempts) {
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
                        return Promise.delay(task.options.retries.delay).then(function () {
                            return self.client.updateMetadata().then(function () {
                                return _try(toRetry, ++attempt);
                            });
                        });
                    }
                    return null;
                });
            });
        });
    }(data))
    .then(function () {
        task.resolve(result);
    })
    .catch(function (err) {
        task.reject(err);
    });
};

/**
 * Send message or messages to Kafka
 *
 * @param  {Object|Array} data [{ topic, partition, message: {key, value, attributes} }]
 * @param  {Object} options { codec, retries: { attempts, delay }, batch: { size } }
 * @return {Promise}
 */
Producer.prototype.send = function (data, options) {
    var self = this, hash, promise, task;

    if (!Array.isArray(data)) {
        data = [data];
    }

    options = _.merge({}, {
        codec: self.options.codec,
        retries: self.options.retries,
        batch: self.options.batch
    }, options || {});

    hash = [
        options.codec,
        options.retries.attempts,
        options.retries.delay,
        options.batch.size,
        options.batch.maxWait,
    ].join('.');

    if (self.queue[hash] === undefined) {
        promise = new Promise(function (resolve, reject) {
            self.queue[hash] = {
                timeout: null,
                resolve: resolve,
                reject: reject,
                options: options,
                data: [],
                dataSize: 0
            };
        });
        self.queue[hash].promise = promise;
    }

    task = self.queue[hash];
    task.data.push(data);
    task.dataSize += _.sumBy(data, _.partialRight(_.get, 'message.value.length', 0));

    if (task.dataSize >= options.batch.size || options.batch.maxWait === 0) {
        if (task.timeout !== null) {
            clearTimeout(task.timeout);
        }
        self._send(hash);
    } else if (task.timeout === null) {
        task.timeout = setTimeout(function () {
            self._send(hash);
        }, options.batch.maxWait);
    }

    return task.promise;
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
