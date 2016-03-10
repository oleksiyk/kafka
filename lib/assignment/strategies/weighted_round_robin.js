'use strict';

var _       = require('lodash');
var WRRPool = require('wrr-pool');
var util    = require('util');
var DefaultAssignmentStrategy = require('./default');

function WeightedRoundRobinAssignmentStrategy() {
}

util.inherits(WeightedRoundRobinAssignmentStrategy, DefaultAssignmentStrategy);

WeightedRoundRobinAssignmentStrategy.prototype.assignment = function (subscriptions) { // [{topic:String, members:[], partitions:[]}]
    var result = [];

    _.each(subscriptions, function (sub) {
        var members = new WRRPool();
        _.each(sub.members, function (member) {
            var weight = 10, m;
            if (Buffer.isBuffer(member.metadata)) {
                m = JSON.parse(member.metadata);
                weight = m.weight;
            }
            members.add(member.id, weight);
        });

        _.each(sub.partitions, function (p) {
            result.push({
                topic: sub.topic,
                partition: p,
                memberId: members.next()
            });
        });
    });

    return result;
};

module.exports = WeightedRoundRobinAssignmentStrategy;
