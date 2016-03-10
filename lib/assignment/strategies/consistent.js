'use strict';

var _        = require('lodash');
var HashRing = require('hashring');
var util     = require('util');
var DefaultAssignmentStrategy = require('./default');

function ConsistentAssignmentStrategy() {
}

util.inherits(ConsistentAssignmentStrategy, DefaultAssignmentStrategy);

ConsistentAssignmentStrategy.prototype.assignment = function (subscriptions) { // [{topic:String, members:[], partitions:[]}]
    var result = [];

    _.each(subscriptions, function (sub) {
        var members = {}, ring;
        _.each(sub.members, function (member) {
            var m;
            if (Buffer.isBuffer(member.metadata)) {
                m = JSON.parse(member.metadata);
                members[m.id] = {
                    _id: member.id,
                    weight: m.weight || 50
                };
            } else {
                // ConsistentAssignmentStrategy requires {id, weight} object in metadata each member
                members[member.id] = {
                    _id: member.id
                };
            }
        });

        ring = new HashRing(members, 'md5', {
            replicas: 3
        });

        _.each(sub.partitions, function (p) {
            result.push({
                topic: sub.topic,
                partition: p,
                memberId: members[ring.get(sub.topic + ':' + p)]._id
            });
        });
    });

    return result;
};

module.exports = ConsistentAssignmentStrategy;
