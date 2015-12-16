"use strict";

var Promise    = require('bluebird');
var Connection = require('./connection');
var protocol   = require('./protocol');
var errors     = require('./errors');
var _          = require('lodash');

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

    this.correlationId = 0;
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

    self.updateMetadata._running = self.metadataRequest().then(function (response) {
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
    })
    .tap(function () {
        self.updateMetadata._running = false;
    });

    return self.updateMetadata._running;
};

Client.prototype.metadataRequest = function(topicNames) {
    var self = this, buffer;

    buffer = self.encoder.reset().MetadataRequest({
        correlationId: self.correlationId++,
        clientId: self.options.id,
        topicNames: topicNames || []
    }).result();

    return Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(buffer).then(function (responseBuffer) {
            return protocol.read(responseBuffer).MetadataResponse().result;
        });
    }));

};

Client.prototype._metadata = function() {
    var self = this;

    if(self.updateMetadata._running){
        return self.updateMetadata._running;
    } else {
        return Promise.resolve();
    }
};

Client.prototype.findLeader = function(topic, partition, notfoundOK) {
    var self = this;

    return self._metadata().then(function () {
        return new Promise(function (resolve, reject) {
            var r = _.get(self.topicMetadata, [topic, partition, 'leader'],
                notfoundOK ? parseInt(_.keys(self.brokerConnections)[0]) : errors.byName('UnknownTopicOrPartition'));
            if(r instanceof Error){
                reject(r);
            } else {
                resolve(r);
            }
        });
    });
};

Client.prototype.produceRequest = function(requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.encoder.reset().ProduceRequest({
                correlationId: self.correlationId++,
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


Client.prototype.fetchRequest = function(requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.encoder.reset().FetchRequest({
                correlationId: self.correlationId++,
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

Client.prototype.offsetRequest = function(requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.encoder.reset().OffsetRequest({
                correlationId: self.correlationId++,
                clientId: self.options.id,
                topics: topics
            }).buffer;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return protocol.read(responseBuffer).OffsetResponse().result.topics;
            });
        }))
        .then(function (results) {
            return _.flatten(results);
        });
    });
};
