"use strict";

var Promise  = require('bluebird');
var _        = require('lodash');
var Client   = require('./client');
var Kafka    = require('./index');
var HashRing = require('hashring');

function GroupConsumer (options){
    this.options = _.partialRight(_.merge, _.defaults)(options || {}, {
        groupId: 'no-kafka-group-v0.9',
        sessionTimeout: 15000 // min 6000, max 30000
    });

    this.client = new Client(this.options);

    this.strategies = {}; // available assignment strategies

    this.leaderId = null;
    this.memberId = null;
    this.generationId = 0;
    this.members = null;

    this.strategy = null; // current strategy assigned by group coordinator
}

module.exports = GroupConsumer;

GroupConsumer.HashRingAssignment = function(subscriptions) { // [{topic:String, members:[], partitions:[]}]
    var result = [];

    _.each(subscriptions, function(sub) {
        var members = {},
            ring;
        _.each(sub.members, function(member) {
            members[member.id] = {
                weight: Buffer.isBuffer(member.metadata) && member.metadata.length === 4 ? member.metadata.readInt32BE(0) : 50
            };
        });

        ring = new HashRing(members, 'md5', {
            compatibility: 'hash_ring'
        });

        _.each(sub.partitions, function(p) {
            result.push({
                topic: sub.topic,
                partition: p,
                memberId: ring.get(sub.topic + ':' + p)
            });
        });
    });

    return result;
};

GroupConsumer.prototype.init = function(strategies) { // [{strategy, version, subscriptions, metadata, fn}]
    var self = this;

    if(_.isEmpty(strategies)){
        return Promise.reject(new Error('Group consumer requires Assignment Strategies to be fully configured'));
    }

    if(!Array.isArray(strategies)){
        strategies = [strategies];
    }

    return this.client.init().then(function () {
        strategies.forEach(function (s) {
            if(typeof s.fn !== 'function'){
                s.fn = GroupConsumer.HashRingAssignment;
            }
            self.strategies[s.strategy] = s;
        });

        return self._joinGroup().then(function () {
            return self._rejoin(); // rejoin with received memberId
        })
        .then(function () {
            self._heartbeat(); // start sending heartbeats
        });
    });
};

GroupConsumer.prototype._joinGroup = function() {
    var self = this;

    function tryJoinGroup (attempt) {
        attempt = attempt || 0;
        if(attempt > 3){
            throw new Error('Failed to join the group');
        }

        return self.client.joinConsumerGroupRequest(self.options.groupId, self.memberId, self.options.sessionTimeout, _.values(self.strategies))
        .catch(function (err) {
            if(err.code === 'GroupCoordinatorNotAvailable'){
                Kafka.log('Waiting for Kafka to create offsets topic');
                return Promise.delay(1000).then(function () {
                    return tryJoinGroup(++attempt);
                });
            }
            throw err;
        });
    }

    return tryJoinGroup()
    .then(function (response) {
        if(self.memberId){
            Kafka.log('Joined group', self.options.groupId, 'as', response.memberId);
            if(response.memberId === response.leaderId){
                Kafka.log('Elected as group leader');
            }
        }
        Kafka.log(response);
        self.memberId = response.memberId;
        self.leaderId = response.leaderId;
        self.generationId = response.generationId;
        self.members = response.members;
        self.strategy = response.groupProtocol;
    });
};

GroupConsumer.prototype._syncGroup = function() {
    var self = this;

    return Promise.try(function () {
        if(self.memberId === self.leaderId){ // leader should generate group assignments
            var r = [];
            _.each(self.members, function (member) {
                _.each(member.subscriptions, function (topic) {
                    r.push([topic, member]);
                });
            });
            r = _(r).groupBy(0).map(function (val, key) {
                return {
                    topic: key,
                    members: _.map(val, 1),
                    partitions: _.map(self.client.topicMetadata[key], 'partitionId')
                };
            }).value();

            return Promise.try(self.strategies[self.strategy].fn, [r]);
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
        Kafka.log(response.memberAssignment.partitionAssignment);
    });
};

GroupConsumer.prototype._rejoin = function() {
    var self = this;

    function _tryRebalance(attempt){
        attempt = attempt || 0;

        if(attempt > 3){
            throw new Error('Failed to rejoin');
        }

        return self._joinGroup().then(function () {
            return self._syncGroup();
        })
        .catch(function (err) {
            if(err.code === 'RebalanceInProgress'){
                return Promise.delay(1000).then(function () {
                    return _tryRebalance(++attempt);
                });
            }
        });
    }

    return _tryRebalance();
};

GroupConsumer.prototype._heartbeat = function() {
    var self = this;

    return self.client.heartbeatRequest(self.options.groupId, self.memberId, self.generationId)
    .catch(function (err) {
        if(err.code === 'RebalanceInProgress'){
            Kafka.log('Rejoining group on RebalanceInProgress');
            return self._rejoin();
        } else if(err.code === 'UnknownMemberId'){
            Kafka.log('Rejoining from scratch UnknownMemberId');
            self.memberId = null;
            return self._joinGroup().then(function () {
                return self._rejoin();
            });
        }
        Kafka.error(err);
    })
    .tap(function () {
        setTimeout(function () {
            self._heartbeat();
        }, 500);
    });
};



