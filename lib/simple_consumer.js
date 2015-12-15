"use strict";

// var Promise = require('bluebird');
var _       = require('lodash');
var Client  = require('./client');
var events  = require("events");
var util    = require("util");
var kafka   = require('./index');

function SimpleConsumer (options){
    this.options = _.partialRight(_.merge, _.defaults)(options || {}, {
        timeout: 100,
        minBytes: 1,
        maxBytes: 1024 * 1024
    });

    this.client = new Client(this.options);

    this.subscriptions = {};

    events.EventEmitter.call(this);
}

module.exports = SimpleConsumer;

util.inherits(SimpleConsumer, events.EventEmitter);

SimpleConsumer.prototype.init = function() {
    return this.client.init();
};

SimpleConsumer.prototype._fetch = function() {
    var self = this, immediate = false;

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
                var s = self.subscriptions[t.topicName + ':' + p.partition];
                if(p.error){
                    kafka.error(p.error);
                    if(p.error.code === 'OffsetOutOfRange'){ // update partition offset
                        return self._offset(s.leader, t.topicName, p.partition, null, kafka.LATEST_OFFSET).then(function (offset) {
                            s.offset = offset;
                        });
                    }
                } else {
                    if(p.messageSet.length){
                        s.offset = _.last(p.messageSet).offset + 1; // advance offset position
                        if(s.offset < p.highwaterMarkOffset){ // more messages available
                            immediate = true;
                        }
                        self.emit('data', p.messageSet, t.topicName, p.partition);
                    }
                }
            });
        });
    })
    .tap(function () {
        self._fetch._running = false;
        var schedule = immediate ? process.nextTick : _.partialRight(setTimeout, 1000);
        schedule(function () {
            self._fetch.call(self);
        });
    });
};

SimpleConsumer.prototype._offset = function(leader, topic, partition, time) {
    var self = this;

    return self.client.offsetRequest({
        0: [{topicName: topic, partitions: [{
            partition: partition,
            time: time || kafka.LATEST_OFFSET, // the latest (next) offset by default
            maxNumberOfOffsets: 1
        }]}]
    })
    .then(function (offsets) {
        // var p = _.chain(offsets).find({topicName: topic}).get('partitions').find({partition: partition}).value();
        var p = offsets[0].partitions[0];
        if(p.error){
            throw p.error;
        }
        return p.offset[0];
    });
};

SimpleConsumer.prototype.subscribe = function(topic, partition, offset, time) {
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
        if(offset >= 0){
            return _subscribe(leader, offset);
        } else {
            return self._offset(leader, topic, partition, time).then(function (offset) {
                return _subscribe(leader, offset);
            });
        }
    })
    .tap(function () {
        process.nextTick(function () {
            self._fetch.call(self);
        });
    });
};

SimpleConsumer.prototype.unsubscribe = function(topic, partition) {
    partition = partition || 0;

    delete this.subscriptions[topic + ':' + partition];
};
