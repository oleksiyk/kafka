'use strict';

var Promise      = require('./bluebird-configured');
var _            = require('lodash');
var BaseConsumer = require('./base_consumer');
var Kafka        = require('./index');
var util         = require('util');
var errors       = require('./errors');

function GroupConsumer(options) {
    this.options = _.defaultsDeep(options || {}, {
        groupId: 'no-kafka-group-v0.9',
        sessionTimeout: 15000, // min 6000, max 30000
        heartbeatTimeout: 1000,
        retentionTime: 24 * 3600 * 1000, // offset retention time, in ms
        startingOffset: Kafka.LATEST_OFFSET
    });

    BaseConsumer.call(this, this.options);

    if (!this.client.validateId(this.options.groupId)) {
        throw new Error('Invalid groupId. Kafka IDs may not contain the following characters: ?:,"');
    }

    this.strategies = {}; // available assignment strategies

    this.leaderId = null;
    this.memberId = null;
    this.generationId = 0;
    this.members = null;
    this.topics = [];

    this.strategyName = null; // current strategy assigned by group coordinator

    this._heartbeatPromise = Promise.resolve();
}

module.exports = GroupConsumer;

util.inherits(GroupConsumer, BaseConsumer);

/**
 * Initialize GroupConsumer
 *
 * @param  {Array|Object} strategies [{name, subscriptions, metadata, strategy, handler}]
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
            if (typeof s.handler !== 'function') {
                throw new Error('Strategy ' + s.name + ' is missing data handler');
            }
            if (s.strategy === undefined) {
                s.strategy = new Kafka.DefaultAssignmentStrategy();
            }
            if (!(s.strategy instanceof Kafka.DefaultAssignmentStrategy)) {
                throw new Error('AssignmentStrategy must inherit from Kafka.DefaultAssignmentStrategy');
            }
            if (s.name === undefined) {
                s.name = s.strategy.constructor.name;
            }
            if (_.isPlainObject(s.metadata)) {
                s.metadata = JSON.stringify(s.metadata);
            }
            s.version = 0;
            self.strategies[s.name] = s;
            self.topics = self.topics.concat(s.subscriptions);
        });

        return self._fullRejoin();
    });
};

GroupConsumer.prototype._joinGroup = function () {
    var self = this;

    return (function _tryJoinGroup(attempt) {
        attempt = attempt || 0;
        if (attempt > 3) {
            throw new Error('Failed to join the group: GroupCoordinatorNotAvailable');
        }

        return self.client.joinConsumerGroupRequest(self.options.groupId, self.memberId, self.options.sessionTimeout, _.values(self.strategies))
        .catch({ code: 'GroupCoordinatorNotAvailable' }, function () {
            return Promise.delay(1000).then(function () {
                return _tryJoinGroup(++attempt);
            });
        });
    }())
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
        self.strategyName = response.groupProtocol;
    });
};

GroupConsumer.prototype._syncGroup = function () {
    var self = this;

    return Promise.try(function () {
        if (self.memberId === self.leaderId) { // leader should generate group assignments
            return self.client.updateMetadata(self.topics).then(function () {
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

                return self.strategies[self.strategyName].strategy.assignment(r);
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

    return (function _tryRebalance(attempt) {
        attempt = attempt || 0;

        if (attempt > 3) {
            throw new Error('Failed to rejoin: RebalanceInProgress');
        }

        return self._joinGroup().then(function () {
            return self._syncGroup();
        })
        .catch({ code: 'RebalanceInProgress' }, function () {
            return Promise.delay(1000).then(function () {
                return _tryRebalance(++attempt);
            });
        });
    }());
};

GroupConsumer.prototype._fullRejoin = function () {
    var self = this;

    return (function _tryFullRejoin() {
        self.memberId = null;
        return self.client.updateGroupCoordinator(self.options.groupId).then(function () {
            return self._joinGroup().then(function () { // join group
                return self._rejoin(); // rejoin and sync with received memberId
            });
        })
        .catch(function (err) {
            self.client.error('Full rejoin attempt failed:', err);
            return Promise.delay(1000).then(_tryFullRejoin);
        });
    }())
    .tap(function () {
        self._heartbeatPromise = self._heartbeat(); // start sending heartbeats
        return null;
    });
};

GroupConsumer.prototype._heartbeat = function () {
    var self = this;

    return self.client.heartbeatRequest(self.options.groupId, self.memberId, self.generationId)
    .catch({ code: 'RebalanceInProgress' }, function () {
        // new group member has joined or existing member has left
        self.client.log('Rejoining group on RebalanceInProgress');
        return self._rejoin();
    })
    .tap(function () {
        self._heartbeatTimeout = setTimeout(function () {
            self._heartbeatPromise = self._heartbeat();
        }, self.options.heartbeatTimeout);
    })
    .catch(function (err) {
        // some severe error, such as GroupCoordinatorNotAvailable or network error
        // in this case we should start trying to rejoin from scratch
        self.client.error('Sending heartbeat failed: ', err);
        return self._fullRejoin().catch(function (_err) {
            self.client.error(_err);
        });
    });
};

/**
 * Leave consumer group and close all connections
 *
 * @return {Promise}
 */
GroupConsumer.prototype.end = function () {
    var self = this;

    self.subscriptions = {};

    self._heartbeatPromise.cancel();
    clearTimeout(self._heartbeatTimeout);

    return self.client.leaveGroupRequest(self.options.groupId, self.memberId)
    .then(function () {
        return BaseConsumer.prototype.end.call(self);
    });
};

GroupConsumer.prototype._prepareOffsetRequest = function (type, commits) {
    if (!Array.isArray(commits)) {
        commits = [commits];
    }

    return _(commits).groupBy('topic').map(function (v, k) {
        return {
            topicName: k,
            partitions: type === 'fetch' ? _.map(v, 'partition') : _.map(v, function (p) {
                p.offset += 1; // commit next offset instead of last consumed
                return p;
            })
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
    if (self.memberId === null) {
        return Promise.reject(errors.byName('RebalanceInProgress'));
    }
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
    return self.client.offsetFetchRequestV1(self.options.groupId, self._prepareOffsetRequest('fetch', commits));
};

GroupConsumer.prototype._updateSubscriptions = function (partitionAssignment) {
    var self = this, offsetRequests = [],
        handler = self.strategies[self.strategyName].handler;

    self.subscriptions = {};

    if (_.isEmpty(partitionAssignment)) {
        return self.client.warn('No partition assignment received');
    }

    // should probably wait for current fetch/handlers to finish before fetching offsets and re-subscribing

    _.each(partitionAssignment, function (a) {
        _.each(a.partitions, function (p) {
            offsetRequests.push({
                topic: a.topic,
                partition: p
            });
        });
    });

    return self.client.updateMetadata(self.topics).then(function () {
        return self.fetchOffset(offsetRequests).map(function (p) {
            var options = {
                offset: p.offset
            };

            if (p.error || p.offset < 0) {
                options = {
                    time: self.options.startingOffset
                };
            }

            return self.subscribe(p.topic, p.partition, options, handler).catch(function (err) {
                self.client.error('Failed to subscribe to', p.topic + ':' + p.partition, err);
            });
        });
    });
};
