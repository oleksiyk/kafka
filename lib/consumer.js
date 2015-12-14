"use strict";

var Promise = require('bluebird');
var _       = require('lodash');
var Client  = require('./client');
var events  = require("events");
var util    = require("util");

function Consumer (options){
    this.options = _.partialRight(_.merge, _.defaults)(options || {}, {
        timeout: 100,
        minBytes: 1,
        maxBytes: 1024 * 1024
    });

    this.client = new Client(this.options);

    this.subscriptions = {};

    events.EventEmitter.call(this);
}

module.exports = Consumer;

util.inherits(Consumer, events.EventEmitter);

Consumer.prototype.init = function() {
    return this.client.init();
};

Consumer.prototype._prepareFetchRequest = function() {
    this._fetchRequestData = _(this.subscriptions).groupBy('leader').mapValues(function (v) {
        return _(v)
            .groupBy('topic')
            .map(function (p, t) {
                return {
                    topicName: t,
                    partitions: p
                };
            })
            .value();
    })
    .value();
};

Consumer.prototype._fetch = function() {
    var self = this;

    if(self._fetch._running){
        return self._fetch._running;
    }

    var data = _(self.subscriptions).values().groupBy('leader').mapValues(function (v) {
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

    return self.client.fetchRequest(data);
};

Consumer.prototype.subscribe = function(topic, partition, offset) {
    var self = this;

    return self.client.findLeader(topic, partition).then(function (leader) {
        partition = partition || 0;
        self.subscriptions[topic + ':' + partition] = {
            topic: topic,
            partition: partition,
            offset: offset || 0,
            leader: leader,
            maxBytes: self.options.maxBytes
        };
    });
};

Consumer.prototype.unsubscribe = function(topic, partition) {
    delete this.subscriptions[topic + ':' + partition];
};
