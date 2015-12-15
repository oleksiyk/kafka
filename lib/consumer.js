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

    if(_.isEmpty(data)){
        self._fetch._running = false;
        return;
    }

    self._fetch._running = self.client.fetchRequest(data).then(function (topics) {
        topics.forEach(function (t) {
            t.partitions.forEach(function (p) {
                if(p.error){
                    console.error(p.error);
                } else {
                    self.subscriptions[t.topicName + ':' + p.partition].offset = p.highwaterMarkOffset;
                    if(p.messageSet.length){
                        console.log(p.highwaterMarkOffset, p.messageSet[p.messageSet.length-1].offset);
                        self.emit('data', p.messageSet, t.topicName, p.partition);
                    }
                }
            });
        });
    })
    .tap(function () {
        self._fetch._running = false;
        process.nextTick(function () {
            self._fetch.call(self);
        });
    });
};

Consumer.prototype.subscribe = function(topic, partition, offset) {
    var self = this;

    partition = partition || 0;

    function _subscribe (leader, _offset) {
        self.subscriptions[topic + ':' + partition] = {
            topic: topic,
            partition: partition,
            offset: _offset,
            leader: leader,
            maxBytes: self.options.maxBytes
        };
        return _offset;
    }

    return self.client.findLeader(topic, partition).then(function (leader) {
        if(offset !== undefined && offset >= 0){
            return _subscribe(leader, offset);
        } else {
            return self.client.offsetRequest({
                0: [{topicName: topic, partitions: [{
                    partition: partition,
                    time: offset || -1, // the latest (next) offset by default
                    maxNumberOfOffsets: 1
                }]}]
            }).then(function (offsets) {
                // var p = _.chain(offsets).find({topicName: topic}).get('partitions').find({partition: partition}).value();
                var p = offsets[0].partitions[0];
                if(p.error){
                    throw p.error;
                }
                return _subscribe(leader, p.offset[0]);
            });
        }
    })
    .tap(function () {
        process.nextTick(function () {
            self._fetch.call(self);
        });
    });
};

Consumer.prototype.unsubscribe = function(topic, partition) {
    partition = partition || 0;

    delete this.subscriptions[topic + ':' + partition];
};
