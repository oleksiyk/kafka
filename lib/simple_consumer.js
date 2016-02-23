'use strict';

var Promise      = require('./bluebird-configured');
var _            = require('lodash');
var BaseConsumer = require('./base_consumer');
var util         = require('util');
// var Kafka        = require('./index');

function SimpleConsumer(options) {
    this.options = _.defaultsDeep(options || {}, {
        groupId: 'no-kafka-group-v0'
    });

    BaseConsumer.call(this, this.options);
}

module.exports = SimpleConsumer;

util.inherits(SimpleConsumer, BaseConsumer);

/**
 * Initialize SimpleConsumer
 *
 * @return {Prommise}
 */
SimpleConsumer.prototype.init = function () {
    return BaseConsumer.prototype.init.call(this);
};

SimpleConsumer.prototype._prepareOffsetRequest = function (type, commits) {
    var self = this;

    if (!Array.isArray(commits)) {
        commits = [commits];
    }

    return Promise.map(commits, function (commit) {
        if (self.subscriptions[commit.topic + ':' + commit.partition]) {
            commit.leader = self.subscriptions[commit.topic + ':' + commit.partition].leader;
            return null;
        }
        return self.client.findLeader(commit.topic, commit.partition).then(function (leader) {
            commit.leader = leader;
        });
    })
    .then(function () {
        return _(commits).groupBy('leader').mapValues(function (v) {
            return _(v)
                .groupBy('topic')
                .map(function (p, t) {
                    return {
                        topicName: t,
                        partitions: type === 'fetch' ? _.map(p, 'partition') : p
                    };
                })
                .value();
        }).value();
    });
};

/**
 * Commit (save) processed offsets to Kafka (Zookeeper)
 *
 * @param  {Object|Array} commits [{topic, partition, offset, metadata}]
 * @return {Promise}
 */
SimpleConsumer.prototype.commitOffset = function (commits) {
    var self = this;

    return self._prepareOffsetRequest('commit', commits).then(function (data) {
        return self.client.offsetCommitRequestV0(self.options.groupId, data);
    });
};

/**
 * Fetch commited (saved) offsets [{topic, partition}]
 *
 * @param  {Object|Array} commits
 * @return {Promise}
 */
SimpleConsumer.prototype.fetchOffset = function (commits) {
    var self = this;

    return self._prepareOffsetRequest('fetch', commits).then(function (data) {
        return self.client.offsetFetchRequestV0(self.options.groupId, data);
    });
};
