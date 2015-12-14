"use strict";

var Promise = require('bluebird');
var _       = require('lodash');
var Client  = require('./client');

function Producer (options){
    this.options = _.partialRight(_.merge, _.defaults)(options || {}, {
        requiredAcks: 1,
        timeout: 100
    });

    this.client = new Client(this.options);
}

module.exports = Producer;

Producer.prototype.init = function() {
    return this.client.init();
};

Producer.prototype._prepareProduceRequest = function(data) {
    var self = this, result = {};

    return Promise.each(data, function (d) {
        return self.client.findLeader(d.topic, d.partition, true).then(function (leader) {
            if(!result[leader]){
                result[leader] = {};
            }
            if(!result[leader][d.topic]){
                result[leader][d.topic] = {};
            }
            if(!result[leader][d.topic]){
                result[leader][d.topic] = {};
            }
            if(!result[leader][d.topic][d.partition]){
                result[leader][d.topic][d.partition] = [];
            }
            result[leader][d.topic][d.partition].push({message: d.message});
        });
    })
    .then(function () {
        return _.mapValues(result, function (topics) {
            return _.map(topics, function (partitions, topic) {
                return {
                    topicName: topic,
                    partitions: _.map(partitions, function (messageSet, partition) {
                        return {
                            partition: parseInt(partition),
                            messageSet: messageSet
                        };
                    })
                };
            });
        });
    });
};

Producer.prototype.send = function(data) { // [{ topic, partition, message: {key, value, attributes}, }]
    var self = this;

    if(!Array.isArray(data)){
        data = [data];
    }

    return self._prepareProduceRequest(data).then(function (requests) {
        return self.client.produceRequest(requests).then(function (response) {
            if(_.isEmpty(response)){
                return response;
            }
            return _.transform(response, function (result, topic) {
                if(!topic){ return }
                _.each(topic.partitions, function (partition) {
                    if(partition.error){
                        result.errors.push({
                            topic: topic.topicName,
                            partition: partition.partition,
                            error: partition.error
                        });
                    } else {
                        result.ok.push({
                            topic: topic.topicName,
                            partition: partition.partition,
                            offset: partition.offset
                        });
                    }
                });
            }, { ok: [], errors: [] });
        });
    });
};
