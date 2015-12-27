"use strict";

var Promise    = require('bluebird');
var Connection = require('./connection');
var protocol   = require('./protocol');
var errors     = require('./errors');
var _          = require('lodash');
var Kafka      = require('./index');

function Client (options){

    this.options = _.partialRight(_.merge, _.defaults)(options || {}, {
        clientId: 'no-kafka-client',
        connectionString: '127.0.0.1:9092'
    });

    this.encoder = new protocol.Writer(256*1024);

    // client metadata
    this.initialBrokers = []; // based on options.connectionString, used for metadata requests
    this.brokerConnections = {};
    this.topicMetadata = {};

    this.correlationId = 0;

    // group metadata
    this.groupCoordinators = {};
}

module.exports = Client;

function _mapTopics (topics) {
    return _(topics).flatten().transform(function(a, tv) {
        _.each(tv.partitions, function(p) {
            a.push(_.merge({
                topic: tv.topicName,
                partition: p.partition
            },
             _.omit(p, 'partition'),
            function(a, b) {if (b instanceof Buffer) {return b;}}) // fix for lodash _merge in Node v4: https://github.com/lodash/lodash/issues/1453
            );
        });
    }, []).value();
}

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

    self._updateMetadata_running = self.metadataRequest().then(function (response) {
        var oldConnections = self.brokerConnections;
        self.brokerConnections = {};

        _.each(response.broker, function (broker) {
            var connection = _.find(oldConnections, function (c, i) {
                return c.equal(broker.host, broker.port) && delete oldConnections[i];
            });
            self.brokerConnections[broker.nodeId] = connection || new Connection({
                host: broker.host,
                port: broker.port,
                auto_connect: true
            });
        });

        _.each(oldConnections, function (c) {c.close()});

        _.each(response.topicMetadata, function (topic) {
            self.topicMetadata[topic.topicName] = {};
            topic.partitionMetadata.forEach(function (partition) {
                self.topicMetadata[topic.topicName][partition.partitionId] = partition;
            });
        });

        if(_.isEmpty(self.brokerConnections)){
            Kafka.warn('No broker metadata received, retrying metadata request in 1000ms');
            return Promise.delay(1000).then(function () {
                return self.updateMetadata();
            });
        }
    })
    .tap(function () {
        self._updateMetadata_running = false;
    });

    return self._updateMetadata_running;
};

Client.prototype.metadataRequest = function(topicNames) {
    var self = this, buffer;

    buffer = self.encoder.reset().MetadataRequest({
        correlationId: self.correlationId++,
        clientId: self.options.clientId,
        topicNames: topicNames || []
    }).result();

    return Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(buffer).then(function (responseBuffer) {
            return protocol.read(responseBuffer).MetadataResponse().result;
        });
    }))
    .catch(function (err) {
        if(err.length === 1){
            throw err[0];
        }
        throw err;
    });
};

Client.prototype._metadata = function() {
    var self = this;

    if(self._updateMetadata_running){
        return self._updateMetadata_running;
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
                if(!self.brokerConnections[r]){
                    return reject(errors.byName('LeaderNotAvailable'));
                }
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
                clientId: self.options.clientId,
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
        .then(_mapTopics);
    });
};


Client.prototype.fetchRequest = function(requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.encoder.reset().FetchRequest({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                timeout: self.options.timeout,
                minBytes: self.options.minBytes,
                topics: topics
            }).buffer;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return protocol.read(responseBuffer).FetchResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetRequest = function(requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.encoder.reset().OffsetRequest({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                topics: topics
            }).buffer;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return protocol.read(responseBuffer).OffsetResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetCommitRequestV0 = function(groupId, requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.encoder.reset().OffsetCommitRequestV0({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                groupId: groupId,
                topics: topics
            }).buffer;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return protocol.read(responseBuffer).OffsetCommitResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};


Client.prototype.offsetFetchRequestV0 = function(groupId, requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.encoder.reset().OffsetFetchRequest({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                apiVersion: 0,
                groupId: groupId,
                topics: topics
            }).buffer;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return protocol.read(responseBuffer).OffsetFetchResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetFetchRequestV2 = function(groupId, requests) {
    var self = this;

    return self._metadata().then(function () {
        return self._findGroupCoordinator(groupId).then(function (connection) {
            var buffer = self.encoder.reset().OffsetFetchRequest({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                apiVersion: 2,
                groupId: groupId,
                topics: requests
            }).buffer;

            return connection.send(buffer).then(function (responseBuffer) {
                return protocol.read(responseBuffer).OffsetFetchResponse().result.topics;
            });
        })
        .then(_mapTopics);
    });
};

Client.prototype._findGroupCoordinator = function(groupId) {
    var self = this;

    if(self.groupCoordinators[groupId] && !self.groupCoordinators[groupId].isRejected()){
        return self.groupCoordinators[groupId];
    }

    var buffer = self.encoder.reset().GroupCoordinatorRequest({
        correlationId: self.correlationId++,
        clientId: self.options.clientId,
        groupId: groupId
    }).buffer;

    self.groupCoordinators[groupId] = Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(buffer).then(function (responseBuffer) {
            var result = protocol.read(responseBuffer).GroupCoordinatorResponse().result;
            if(result.error){
                throw result.error;
            }
            return result;
        });
    }))
    .then(function (host) {
        return new Connection({
            host: host.coordinatorHost,
            port: host.coordinatorPort,
            auto_connect: true
        });
    })
    .catch(function (err) {
        if(err.length === 1){
            throw err[0];
        }
        throw err;
    });

    return self.groupCoordinators[groupId];
};

Client.prototype.joinConsumerGroupRequest = function(groupId, memberId, sessionTimeout, strategies) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.encoder.reset().JoinConsumerGroupRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId,
            groupId: groupId,
            sessionTimeout: sessionTimeout,
            memberId: memberId || '',
            groupProtocols: strategies
        }).buffer;

        return connection.send(buffer).then(function (responseBuffer) {
            var result = protocol.read(responseBuffer).JoinConsumerGroupResponse().result;
            if(result.error){
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.heartbeatRequest = function(groupId, memberId, generationId) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.encoder.reset().HeartbeatRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId,
            generationId: generationId
        }).buffer;

        return connection.send(buffer).then(function (responseBuffer) {
            var result = protocol.read(responseBuffer).HeartbeatResponse().result;
            if(result.error){
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.syncConsumerGroupRequest = function(groupId, memberId, generationId, groupAssignment) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.encoder.reset().SyncConsumerGroupRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId,
            generationId: generationId,
            groupAssignment: groupAssignment
        }).buffer;

        return connection.send(buffer).then(function (responseBuffer) {
            var result = protocol.read(responseBuffer).SyncConsumerGroupResponse().result;
            if(result.error){
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.leaveGroupRequest = function(groupId, memberId) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.encoder.reset().LeaveGroupRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId
        }).buffer;

        return connection.send(buffer).then(function (responseBuffer) {
            var result = protocol.read(responseBuffer).LeaveGroupResponse().result;
            if(result.error){
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.offsetCommitRequestV2 = function(groupId, memberId, generationId, requests) {
    var self = this;

    return self._metadata().then(function () {
        return self._findGroupCoordinator(groupId).then(function (connection) {
            var buffer = self.encoder.reset().OffsetCommitRequestV2({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                groupId: groupId,
                generationId: generationId,
                memberId: memberId,
                retentionTime: self.options.retentionTime,
                topics: requests
            }).buffer;

            return connection.send(buffer).then(function (responseBuffer) {
                return protocol.read(responseBuffer).OffsetCommitResponse().result.topics;
            });
        })
        .then(_mapTopics);
    });
};
