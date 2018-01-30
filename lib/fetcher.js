'use strict';

var Promise = require('./bluebird-configured');
var _       = require('lodash');
var errors  = require('./errors');
var async   = require('async');

function buildFetchRequests(subscriptions) {
    return _(subscriptions).reject({ paused: true }).values().groupBy('leader').mapValues(function (v) {
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
}

function pauseFetchRequestSubscriptions(subscriptions, requests, paused) {
    _.each(requests, function (request) {
        _.each(request, function (topic) {
            _.each(topic.partitions, function (p) {
                var s = subscriptions[p.topic + ':' + p.partition];
                if (s) {
                    s.paused = paused;
                }
            });
        });
    });
}

function Fetcher(consumer) {
    this.consumer = consumer;
    this.client = consumer.client;
    this.options = consumer.options;
    this.stopping = false;
    this.fetching = false;
    this._fetchPromise = null;
}

module.exports = Fetcher;

Fetcher.prototype._processPartitionData = function (p) {
    var s, self = this;

    if (self.stopping) {
        return Promise.resolve(null);
    }

    s = self.consumer.subscriptions[p.topic + ':' + p.partition];
    if (!s) {
        return Promise.resolve(null); // already unsubscribed while we were polling
    }
    if (p.error) {
        s.paused = false;
        return self._partitionError(p.error, p.topic, p.partition);
    }
    if (p.messageSet.length) {
        return s.handler(p.messageSet, p.topic, p.partition, p.highwaterMarkOffset)
        .catch(function (err) {
            self.client.warn('Handler for', p.topic + ':' + p.partition, 'failed with', err);
        })
        .finally(function () {
            s.paused = false;
            s.offset = _.last(p.messageSet).offset + 1; // advance offset position
        });
    }
    s.paused = false;
    return Promise.resolve(null);
};

Fetcher.prototype._tryStop = function () {
    if (this._resolvePromise) {
        this._resolvePromise(null);
        this._resolvePromise = null;
    }
};

Fetcher.prototype._fetch = function () {
    var self = this, subscriptions, requests;

    // TODO can this be removed?
    if (self.stopping) {
        self._tryStop();
        return;
    }

    try {
        subscriptions = self.consumer.subscriptions;
        requests = buildFetchRequests(subscriptions);

        if (_.isEmpty(requests)) {
            self._scheduleFetch();
            return;
        }

        pauseFetchRequestSubscriptions(subscriptions, requests, true);

        self.fetching = true;
        self.client.fetchRequest(requests).map(function (p) {
            self._queue.push({ p: p }, function (err) {
                if (err) {
                    self.client.error(err);
                }

                self._scheduleFetch();
            });
            return null;
        })
        .catch(function (err) {
            pauseFetchRequestSubscriptions(subscriptions, requests, false);
            throw err;
        })
        .finally(function () {
            self.fetching = false;
        });
    } catch (err) {
        self.client.error(err);
    }
};

Fetcher.prototype._scheduleFetch = function () {
    var self = this;

    if (self.stopping) {
        self._tryStop();
        return;
    }

    if (!self._fetchTimeout) {
        self._fetchTimeout = setTimeout(function () {
            self._fetchTimeout = null;
            self._fetch();
        }, self.options.idleTimeout);
    }
};

Fetcher.prototype.start = function () {
    var self = this;

    self._queue = async.queue(function (task, cb) {
        if (self.stopping) {
            cb();
            return;
        }

        self._processPartitionData(task.p)
        .then(function () {
            cb();
        })
        .catch(function (err) {
            cb(err);
        });
    }, self.options.handlerConcurrency);

    this._fetchPromise = new Promise(function (resolve) {
        self._resolvePromise = resolve;

        self._queue.drain = function () {
            if (self.stopping) {
                self._tryStop();
            }
        };

        self._fetch();
    });
};

Fetcher.prototype.stop = function () {
    this.stopping = true;

    if (this._fetchTimeout) {
        clearTimeout(this._fetchTimeout);
        this._fetchTimeout = null;
    }

    if (!this.fetching) {
        this._tryStop();
    }

    return this._fetchPromise;
};

Fetcher.prototype._partitionError = function (err, topic, partition) {
    var self = this;

    var s = self.consumer.subscriptions[topic + ':' + partition];

    if (err.code === 'OffsetOutOfRange') { // update partition offset to options.recoveryOffset
        self.client.warn('Updating offset because of OffsetOutOfRange error for', topic + ':' + partition);
        return self.client.getPartitionOffset(s.leader, topic, partition, null, self.options.recoveryOffset).then(function (offset) {
            s.offset = offset;
        });
    } else if (err.code === 'MessageSizeTooLarge') {
        self.client.warn('Received MessageSizeTooLarge error for', topic + ':' + partition,
            'which means maxBytes option value (' + s.maxBytes + ') is too small to fit the message at offset', s.offset);
        s.offset += 1;
        return Promise.resolve(null);
    /* istanbul ignore next */
    } else if (/UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable/.test(err.code)) {
        self.client.debug('Received', err.code, 'error for', topic + ':' + partition);
        return self._updateSubscription(topic, partition);
    /* istanbul ignore next */
    } else if (err instanceof errors.NoKafkaConnectionError) {
        self.client.debug('Received', err.toString(), 'for', topic + ':' + partition);
        return self._updateSubscription(topic, partition);
    }
    self.client.error(topic + ':' + partition, err);

    return Promise.resolve(null);
};

/* istanbul ignore next */
Fetcher.prototype._updateSubscription = function (topic, partition) {
    var self = this;

    return self.client.updateMetadata().then(function () {
        var s = self.consumer.subscriptions[topic + ':' + partition];

        return self.subscribe(topic, partition, s.offset !== undefined ? { offset: s.offset } : s.options, s.handler)
        .catch(function (err) {
            self.client.error('Failed to re-subscribe to', topic + ':' + partition, err);
        });
    });
};

// vim: set sw=4 ts=4 expandtab:

