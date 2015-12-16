"use strict";

var Promise = require('bluebird');
var _       = require('lodash');
var Client  = require('./client');
var events  = require("events");
var util    = require("util");
var Kafka   = require('./index');

function SimpleConsumer (options){
    this.options = _.partialRight(_.merge, _.defaults)(options || {}, {
        timeout: 100,
        idleTimeout: 1000,
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
        return Promise.each(topics, function (t) {
            return Promise.each(t.partitions, function (p) {
                var s = self.subscriptions[t.topicName + ':' + p.partition];
                if(p.error){
                    Kafka.error(p.error);
                    if(p.error.code === 'OffsetOutOfRange'){ // update partition offset
                        return self._offset(s.leader, t.topicName, p.partition, null, Kafka.LATEST_OFFSET).then(function (offset) {
                            s.offset = offset;
                        });
                    }
                } else if(p.messageSet.length){
                    s.offset = _.last(p.messageSet).offset + 1; // advance offset position
                    if(s.offset < p.highwaterMarkOffset){ // more messages available
                        immediate = true;
                    }
                    self.emit('data', p.messageSet, t.topicName, p.partition);
                }
            });
        }).return(topics);
    })
    .tap(function () {
        var schedule = immediate ? process.nextTick : _.partialRight(setTimeout, self.options.idleTimeout);
        schedule(function () {
            self._fetch._running = false;
            self._fetch.call(self);
        });
    });

    return self._fetch._running;
};

SimpleConsumer.prototype._offset = function(leader, topic, partition, time) {
    var self = this;

    return self.client.offsetRequest({
        0: [{topicName: topic, partitions: [{
            partition: partition,
            time: time || Kafka.LATEST_OFFSET, // the latest (next) offset by default
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

SimpleConsumer.prototype.offset = function(topic, partition, time) {
    var self = this;

    partition = partition || 0;

    return self.client.findLeader(topic, partition).then(function (leader) {
        return self._offset(leader, topic, partition, time);
    });
};

SimpleConsumer.prototype.subscribe = function(topic, partition, options) {
    var self = this;

    partition = partition || 0;
    options = options || {};

    function _subscribe (leader, _offset) {
        self.subscriptions[topic + ':' + partition] = {
            topic: topic,
            partition: partition,
            offset: _offset,
            leader: leader,
            maxBytes: options.maxBytes || self.options.maxBytes
        };
        return _offset;
    }

    return self.client.findLeader(topic, partition).then(function (leader) {
        if(options.offset >= 0){
            return _subscribe(leader, options.offset);
        } else {
            return self._offset(leader, topic, partition, options.time).then(function (offset) {
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
