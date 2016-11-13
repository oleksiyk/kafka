'use strict';

var _ = require('lodash');

function DefaultAssignmentStrategy() {
}

// simple round robin assignment strategy
DefaultAssignmentStrategy.prototype.assignment = function (subscriptions) { // [{topic:String, members:[], partitions:[]}]
    var result = [];

    _.each(subscriptions, function (sub) {
        _.each(sub.partitions, function (p) {
            result.push({
                topic: sub.topic,
                partition: p,
                memberId: sub.members[p % sub.members.length].id
            });
        });
    });

    return result;
};

module.exports = DefaultAssignmentStrategy;
