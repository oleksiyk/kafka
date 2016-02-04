'use strict';

var Promise      = require('bluebird');
var _            = require('lodash');
var BaseConsumer = require('./base_consumer');
var Kafka        = require('./index');
var util         = require('util');

function GroupConsumer(options) {
    this.options = _.defaultsDeep(options || {}, {
        groupId: 'no-kafka-group-v0.9',
        sessionTimeout: 15000, // min 6000, max 30000
        heartbeatTimeout: 1000,
        retentionTime: 24 * 3600 * 1000, // offset retention time, in ms
        startingOffset: Kafka.LATEST_OFFSET
    });

    BaseConsumer.call(this, this.options);

    this.strategies = {}; // available assignment strategies

    this.leaderId = null;
    this.memberId = null;
    this.generationId = 0;
    this.members = null;

    this.strategy = null; // current strategy assigned by group coordinator
}

module.exports = GroupConsumer;

util.inherits(GroupConsumer, BaseConsumer);

/**
 * Initialize GroupConsumer
 *
 * @param  {Array|Object} strategies [{strategy, subscriptions, metadata, fn}]
 * @return {Promise}
 */
GroupConsumer.prototype.init = function (strategies) {
    var self = this;

    return BaseConsumer.prototype.init.call(self).then(function () {
        if (_.isEmpty(strategies)) {
            throw new Error('Group consumer requires Assignment Strategies to be fully configured');
        }

        if (!Array.isArray(strategies)) {
            strategies = [strategies];
        }

        strategies.forEach(function (s) {
            if (typeof s.fn !== 'function') {
                s.fn = Kafka.RoundRobinAssignment;
            }
            if (_.isPlainObject(s.metadata)) {
                s.metadata = JSON.stringify(s.metadata);
            }
            s.version = 0;
            self.strategies[s.strategy] = s;
        });

        return self._fullRejoin()
        .tap(function () {
            self._heartbeat(); // start sending heartbeats
        });
    });
};

GroupConsumer.prototype._joinGroup = function () {
    var self = this;

    function tryJoinGroup(attempt) {
        attempt = attempt || 0;
        if (attempt > 3) {
            throw new Error('Failed to join the group: GroupCoordinatorNotAvailable');
        }

        return self.client.joinConsumerGroupRequest(self.options.groupId, self.memberId, self.options.sessionTimeout, _.values(self.strategies))
        .catch({ code: 'GroupCoordinatorNotAvailable' }, function () {
            return Promise.delay(1000).then(function () {
                return tryJoinGroup(++attempt);
            });
        });
    }

    return tryJoinGroup()
    .then(function (response) {
        if (self.memberId) {
            self.client.log('Joined group', self.options.groupId, 'generationId', response.generationId, 'as', response.memberId);
            if (response.memberId === response.leaderId) {
                self.client.log('Elected as group leader');
            }
        }
        self.memberId = response.memberId;
        self.leaderId = response.leaderId;
        self.generationId = response.generationId;
        self.members = response.members;
        self.strategy = response.groupProtocol;
    });
};

GroupConsumer.prototype._syncGroup = function () {
    var self = this;

    return Promise.try(function () {
        if (self.memberId === self.leaderId) { // leader should generate group assignments
            return self.client.updateMetadata().then(function () {
                var r = [];
                _.each(self.members, function (member) {
                    _.each(member.subscriptions, function (topic) {
                        r.push([topic, member]);
                    });
                });
                r = _(r).groupBy(0).map(function (val, key) {
                    if (!self.client.topicMetadata[key]) {
                        self.client.error('Sync group: unknown topic:', key);
                    }
                    return {
                        topic: key,
                        members: _.map(val, 1),
                        partitions: _.map(self.client.topicMetadata[key], 'partitionId')
                    };
                }).value();

                return self.strategies[self.strategy].fn(r);
            });
        }
        return [];
    })
    .then(function (result) {
        var assignments = _(result).groupBy('memberId').mapValues(function (mv, mk) {
            return {
                memberId: mk,
                memberAssignment: {
                    version: 0,
                    metadata: null,
                    partitionAssignment: _(mv).groupBy('topic').map(function (tv, tk) {
                        return {
                            topic: tk,
                            partitions: _.map(tv, 'partition')
                        };
                    }).value()
                }
            };
        }).values().value();

        // console.log(require('util').inspect(assignments, true, 10, true));
        return self.client.syncConsumerGroupRequest(self.options.groupId, self.memberId, self.generationId, assignments);
    })
    .then(function (response) {
        return self._updateSubscriptions(_.get(response, 'memberAssignment.partitionAssignment', []));
    });
};

GroupConsumer.prototype._rejoin = function () {
    var self = this;

    function _tryRebalance(attempt) {
        attempt = attempt || 0;

        if (attempt > 3) {
            throw new Error('Failed to rejoin');
        }

        return self._joinGroup().then(function () {
            return self._syncGroup();
        })
        .catch({ code: 'RebalanceInProgress' }, function () {
            return Promise.delay(1000).then(function () {
                return _tryRebalance(++attempt);
            });
        });
    }

    return _tryRebalance();
};

GroupConsumer.prototype._fullRejoin = function () {
    var self = this;

    function _try(attempt) {
        attempt = attempt || 0;

        self.memberId = null;
        return self.client.updateGroupCoordinator(self.options.groupId).then(function () {
            return self._joinGroup().then(function () {
                return self._rejoin(); // rejoin with received memberId
            });
        })
        .catch({ code: 'UnknownMemberId' }, { code: 'NotCoordinatorForGroup' }, function (err) {
            return Promise.delay(1000).then(function () {
                if (attempt > 3) {
                    throw new Error('Failed to do full rejoin, last error was', err);
                }
                return _try(++attempt);
            });
        });
    }

    return _try();
};

GroupConsumer.prototype._heartbeat = function () {
    var self = this;

    if (self._finished) { return Promise.resolve(); }

    return self.client.heartbeatRequest(self.options.groupId, self.memberId, self.generationId)
    .catch({ code: 'RebalanceInProgress' }, function () {
        if (self._finished) { return null; }
        self.client.log('Rejoining group on RebalanceInProgress');
        return self._rejoin();
    })
    .catch(function (err) {
        if (self._finished) { return null; }
        self.client.error('Sending heartbeat failed: ', err);
        return self._fullRejoin().catch(function (_err) {
            self.client.error(_err);
        });
    })
    .tap(function () {
        if (self._finished) { return; }
        setTimeout(function () {
            self._heartbeat();
        }, self.options.heartbeatTimeout);
    });
};

/**
 * Leave consumer group and close all connections
 *
 * @return {Promise}
 */
GroupConsumer.prototype.end = function () {
    var self = this;

    self._finished = true;
    self.subscriptions = {};

    return self.client.leaveGroupRequest(self.options.groupId, self.memberId).then(function () {
        return self.client.end();
    });
};

GroupConsumer.prototype._prepareOffsetRequest = function (type, commits) {
    if (!Array.isArray(commits)) {
        commits = [commits];
    }

    return _(commits).groupBy('topic').map(function (v, k) {
        return {
            topicName: k,
            partitions: type === 'fetch' ? _.map(v, 'partition') : v
        };
    }).value();
};

/**
 * Commit (save) processed offsets to Kafka
 *
 * @param  {Object|Array} commits [{topic, partition, offset, metadata}]
 * @return {Promise}
 */
GroupConsumer.prototype.commitOffset = function (commits) {
    var self = this;
    return self.client.offsetCommitRequestV2(self.options.groupId, self.memberId, self.generationId,
        self._prepareOffsetRequest('commit', commits));
};

/**
 * Fetch commited (saved) offsets [{topic, partition}]
 *
 * @param  {Object|Array} commits
 * @return {Promise}
 */
GroupConsumer.prototype.fetchOffset = function (commits) {
    var self = this;
    return self.client.offsetFetchRequestV2(self.options.groupId, self._prepareOffsetRequest('fetch', commits));
};

GroupConsumer.prototype._updateSubscriptions = function (partitionAssignment) {
    var self = this, offsetRequests = [];

    if (_.isEmpty(partitionAssignment)) {
        return self.client.warn('No partition assignment received');
    }

    _.each(partitionAssignment, function (a) {
        _.each(a.partitions, function (p) {
            offsetRequests.push({
                topic: a.topic,
                partition: p
            });
        });
    });

    self.subscriptions = {};

    return self.client.updateMetadata().then(function () {
        return self.fetchOffset(offsetRequests).then(function (result) {
            return Promise.map(result, function (p) {
                return (function () {
                    if (p.error || p.offset < 0) {
                        return self.subscribe(p.topic, p.partition, {
                            time: self.options.startingOffset
                        });
                    }
                    return self.subscribe(p.topic, p.partition, {
                        offset: p.offset + 1
                    });
                }())
                .catch(function (err) {
                    self.client.error('Failed to subscribe to', p.topic + ':' + p.partition, err);
                });
            });
        });
    });
};
