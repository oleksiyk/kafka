"use strict";

var Promise    = require('bluebird');
var Connection = require('./connection');
var protocol   = require('./protocol');
var _ = require('lodash');

function Client (options){

    this.options = _.partialRight(_.merge, _.defaults)(options || {}, {
        id: 'KafkaNodeClient',
        connectionString: '127.0.0.1:9092'
    });

    this.encoder = new protocol.Writer(256*1024);

    // client metadata
    this.initialBrokers = []; // based on options.connectionString, used for metadata requests
    this.brokerConnections = {};
    this.topicMetadata = {};
}

module.exports = Client;

Client.prototype.init = function() {
    var self = this;

    self.initialBrokers = self.options.connectionString.split(',').map(function (hostStr) {
        var h = hostStr.trim().split(':');

        return new Connection({
            host: h[0],
            port: parseInt(h[1]),
            auto_connect: true
        });
    });

    if(_.isEmpty(self.initialBrokers)){
        return Promise.reject(new Error('No initial hosts to connect'));
    }

    return self.updateMetadata();
};

Client.prototype.updateMetadata = function() {
    var self = this;

    return self.metadataRequest().then(function (response) {
        response.broker.forEach(function (broker) {
            if(self.brokerConnections[broker.nodeId]){
                self.brokerConnections[broker.nodeId].close();
            }
            self.brokerConnections[broker.nodeId] = new Connection({
                host: broker.host,
                port: broker.port,
                auto_connect: true
            });
        });

        response.topicMetadata.forEach(function (topic) {
            self.topicMetadata[topic.topicName] = {};
            topic.partitionMetadata.forEach(function (partition) {
                self.topicMetadata[topic.topicName][partition.partitionId] = partition;
            });
        });
    });
};

Client.prototype.metadataRequest = function(topicNames) {
    var self = this, buffer;

    buffer = self.encoder.reset().MetadataRequest({
        correlationId: 0,
        clientId: self.options.id,
        topicNames: topicNames || []
    }).result();

    return Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(buffer).then(function (responseBuffer) {
            return protocol.read(responseBuffer).MetadataResponse().result;
        });
    }));

};

Client.prototype._prepareProduceRequest = function(data) {
    var self = this, result = {};

    // split messages by leader selected by topic and partition
    // for missing topic/partition try to update metadata and try again
    // if the topic/partition is still missing - just send it to first known broker
    return Promise.each(data, function (d) {
        return (function fill(force) {
            var leader = _.get(self.topicMetadata, [d.topic, d.partition, 'leader']);

            if((leader === undefined || !self.brokerConnections[leader])){
                if(!force){
                    return self.updateMetadata().then(function () {
                        return fill(true); // force
                    });
                } else {
                    leader = parseInt(_.keys(self.brokerConnections)[0]); // just pick a first broker connection, it will fail anyway
                }
            }

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
        })();
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

Client.prototype.produceRequest = function(data) {
    var self = this;

    return self._prepareProduceRequest(data).then(function (requests) {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.encoder.reset().ProduceRequest({
                correlationId: 0,
                clientId: self.options.id,
                requiredAcks: self.options.requiredAcks,
                timeout: self.options.timeout,
                topics: topics
            }).result();

            return self.brokerConnections[leader].send(buffer, self.options.requiredAcks === 0).then(function (responseBuffer) {
                if(self.options.requiredAcks !== 0){
                    return protocol.read(responseBuffer).ProduceResponse().result.topics;
                }
            });
        }))
        .then(function (results) {
            return _.flatten(results);
        });
    });
};

Client.prototype._prepareFetchRequest = function(data) {
    var self = this, result = {};

    return Promise.all(_.map(data, function (partitions, topic) {
        return Promise.all(_.map(partitions, function (offset, partition) {
            return (function fill(force) {
                var leader = _.get(self.topicMetadata, [topic, partition, 'leader']);

                if((leader === undefined || !self.brokerConnections[leader])){
                    if(!force){
                        return self.updateMetadata().then(function () {
                            return fill(true); // force
                        });
                    } else {
                        leader = parseInt(_.keys(self.brokerConnections)[0]);
                    }
                }

                if(!result[leader]){
                    result[leader] = {};
                }
                if(!result[leader][topic]){
                    result[leader][topic] = {};
                }
                if(!result[leader][topic]){
                    result[leader][topic] = {};
                }
                result[leader][topic][partition] = offset;
            })();
        }));
    }))
    .then(function () {
        return _.mapValues(result, function (topics) {
            return _.map(topics, function (partitions, topic) {
                return {
                    topicName: topic,
                    partitions: _.map(partitions, function (offset, partition) {
                        return {
                            partition: parseInt(partition),
                            offset: offset,
                            maxBytes: 329 // self.options.maxBytes
                        };
                    })
                };
            });
        });
    });
};

Client.prototype.fetchRequest = function(data) {
    var self = this;

    return self._prepareFetchRequest(data).then(function (requests) {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.encoder.reset().FetchRequest({
                correlationId: 0,
                clientId: self.options.id,
                timeout: self.options.timeout,
                minBytes: self.options.minBytes,
                topics: topics
            }).buffer;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return protocol.read(responseBuffer).FetchResponse().result.topics;
            });
        }))
        .then(function (results) {
            return _.flatten(results);
        });
    });
};
