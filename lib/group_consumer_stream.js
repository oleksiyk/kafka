'use strict';

var ReadableStream = require('readable-stream').Readable;
var Promise = require('./bluebird-configured');
var _ = require('lodash');
var GroupConsumer = require('./group_consumer');
var through2 = require('through2');

function GroupConsumerStream(options) {
    this.readableStream = new ReadableStream({ objectMode: true });
    this.readableStream._read = function () {};

    this._groupConsumer = new GroupConsumer(options);
    this._end = _.bind(this._groupConsumer.end, this._groupConsumer);
    this._init = _.bind(this._groupConsumer.init, this._groupConsumer);
    this._commitOffset = _.bind(this._groupConsumer.commitOffset, this._groupConsumer);
    this.fetchOffset = _.bind(this._groupConsumer.fetchOffset, this._groupConsumer);
}

module.exports = GroupConsumerStream;

// letting users set handler doesn't make sense in the context of a stream
GroupConsumerStream.prototype.getStream = function (strategies) {
    var self = this;

    var handler = function (messageSet, topic, partition) {
        return self.readableStream.push({
            messageSet: messageSet,
            topic: topic,
            partition: partition
        });
    };

    strategies = strategies.map(function (strategy) {
        strategy.handler = handler;
        return strategy;
    });
    self._init(strategies);
    return this.readableStream;
};

GroupConsumerStream.prototype.commitOffsetTransform = function () {
    var self = this;

    return through2.obj(function (data, enc, callback) {
        return Promise.all(data.messageSet.map(function (m) {
            return self._commitOffset({ topic: data.topic, partition: data.partition, offset: m.offset });
        }))
            .then(function () {
                callback(null, data);
            });
    });
};

GroupConsumerStream.prototype.end = function () {
    var self = this;

    return this._end()
        .then(function () {
            self.readableStream.emit('close');
        });
};
