'use strict';

var _        = require('lodash');
var HashRing = require('hashring');
var WRRPool  = require('wrr-pool');

exports.ConsistentAssignment = function (subscriptions) { // [{topic:String, members:[], partitions:[]}]
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
                // ConsistentAssignment requires {id, weight} object in metadata each member
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

exports.RoundRobinAssignment = function (subscriptions) { // [{topic:String, members:[], partitions:[]}]
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

exports.WeightedRoundRobinAssignment = function (subscriptions) { // [{topic:String, members:[], partitions:[]}]
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

module.exports = (function () {
    exports.Producer = require('./producer');
    exports.SimpleConsumer = require('./simple_consumer');
    exports.GroupConsumer = require('./group_consumer');
    exports.GroupAdmin = require('./group_admin');

    // offset request time constants
    exports.EARLIEST_OFFSET = -2;
    exports.LATEST_OFFSET = -1;

    return exports;
}());
