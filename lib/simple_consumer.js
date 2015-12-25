"use strict";

// var Promise      = require('bluebird');
var _            = require('lodash');
var BaseConsumer = require('./base_consumer');
var util         = require("util");
// var Kafka        = require('./index');

function SimpleConsumer (options){
    this.options = _.partialRight(_.merge, _.defaults)(options || {}, {
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
SimpleConsumer.prototype.init = function() {
    return BaseConsumer.prototype.init.call(this);
};

/**
 * Commit (save) processed offsets to Kafka (Zookeeper)
 *
 * @param  {Object|Array} commits [{topic, partition, offset, metadata}]
 * @return {Promise}
 */
SimpleConsumer.prototype.commitOffset = function(commits) {
    var self = this;

    if(!Array.isArray(commits)){
        commits = [commits];
    }

    var data = _(commits).filter(function (commit) {
        if(self.subscriptions[commit.topic + ':' + commit.partition]){
            commit.leader = self.subscriptions[commit.topic + ':' + commit.partition].leader;
            return true;
        }
    }).groupBy('leader').mapValues(function (v) {
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

    return self.client.offsetCommitRequestV0(self.options.groupId, data);
};

/**
 * Fetch commited (saved) offsets [{topic, partition}]
 *
 * @param  {Object|Array} commits
 * @return {Promise}
 */
SimpleConsumer.prototype.fetchOffset = function(commits) { //
    var self = this;

    if(!Array.isArray(commits)){
        commits = [commits];
    }

    var data = _(commits).filter(function (commit) {
        if(self.subscriptions[commit.topic + ':' + commit.partition]){
            commit.leader = self.subscriptions[commit.topic + ':' + commit.partition].leader;
            return true;
        }
    }).groupBy('leader').mapValues(function (v) {
        return _(v)
            .groupBy('topic')
            .map(function (p, t) {
                return {
                    topicName: t,
                    partitions: _.map(p, 'partition')
                };
            })
            .value();
    }).value();

    return self.client.offsetFetchRequest(self.options.groupId, data);
};

