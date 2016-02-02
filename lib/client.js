'use strict';

var Promise    = require('bluebird');
var Connection = require('./connection');
var Protocol   = require('./protocol');
var errors     = require('./errors');
var _          = require('lodash');
var logger     = require('nice-simple-logger');

function Client(options) {
    var self = this;

    self.options = _.defaultsDeep(options || {}, {
        clientId: 'no-kafka-client',
        connectionString: '127.0.0.1:9092',
        logger: logger(options ? options.nsl : {})
    });

    // prepend clientId argument
    ['log', 'debug', 'error', 'warn'].forEach(function (m) {
        if (typeof self.options.logger[m] === 'function') {
            self[m] = _.partial(self.options.logger[m], self.options.clientId);
        }
    });

    self.protocol = new Protocol({
        bufferSize: 256 * 1024
    });

    // client metadata
    self.initialBrokers = []; // based on options.connectionString, used for metadata requests
    self.brokerConnections = {};
    self.topicMetadata = {};

    self.correlationId = 0;

    // group metadata
    self.groupCoordinators = {};
}

module.exports = Client;

function _mapTopics(topics) {
    return _(topics).flatten().transform(function (a, tv) {
        if (tv === undefined) { return false; } // requiredAcks=0
        _.each(tv.partitions, function (p) {
            a.push(_.merge({
                topic: tv.topicName,
                partition: p.partition
            },
             _.omit(p, 'partition')
            /*function (_a, b) {if (b instanceof Buffer) {return b;}}*/) // fix for lodash _merge in Node v4: https://github.com/lodash/lodash/issues/1453
            );
        });
    }, []).value();
}

Client.prototype.init = function () {
    var self = this;

    self.initialBrokers = self.options.connectionString.split(',').map(function (hostStr) {
        var h = hostStr.trim().split(':');

        return new Connection({
            host: h[0],
            port: parseInt(h[1])
        });
    });

    if (_.isEmpty(self.initialBrokers)) {
        return Promise.reject(new Error('No initial hosts to connect'));
    }

    return self.updateMetadata();
};

Client.prototype.end = function () {
    var self = this;

    return Promise.map(
        Array.prototype.concat(self.initialBrokers, _.values(self.brokerConnections), _.values(self.groupCoordinators)),
        function (c) {
            return c.close();
        });
};

Client.prototype.updateMetadata = function () {
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
                port: broker.port
            });
        });

        _.each(oldConnections, function (c) {c.close();});

        _.each(response.topicMetadata, function (topic) {
            self.topicMetadata[topic.topicName] = {};
            topic.partitionMetadata.forEach(function (partition) {
                self.topicMetadata[topic.topicName][partition.partitionId] = partition;
            });
        });

        if (_.isEmpty(self.brokerConnections)) {
            self.warn('No broker metadata received, retrying metadata request in 1000ms');
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

Client.prototype.metadataRequest = function (topicNames) {
    var self = this, buffer;

    buffer = self.protocol.write().MetadataRequest({
        correlationId: self.correlationId++,
        clientId: self.options.clientId,
        topicNames: topicNames || []
    }).result;

    return Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(buffer).then(function (responseBuffer) {
            return self.protocol.read(responseBuffer).MetadataResponse().result;
        });
    }))
    .catch(function (err) {
        if (err.length === 1) {
            throw err[0];
        }
        throw err;
    });
};

Client.prototype._metadata = function () {
    var self = this;

    if (self._updateMetadata_running) {
        return self._updateMetadata_running;
    }
    return Promise.resolve();
};

Client.prototype.findLeader = function (topic, partition, notfoundOK) {
    var self = this;

    return self._metadata().then(function () {
        return new Promise(function (resolve, reject) {
            var r = _.get(self.topicMetadata, [topic, partition, 'leader'],
                notfoundOK ? parseInt(_.keys(self.brokerConnections)[0]) : errors.byName('UnknownTopicOrPartition'));
            if (r instanceof Error) {
                reject(r);
            } else {
                if (!self.brokerConnections[r]) {
                    return reject(errors.byName('LeaderNotAvailable'));
                }
                resolve(r);
            }
        });
    });
};

function _fakeTopicsErrorResponse(topics, error) {
    return _.map(topics, function (t) {
        return {
            topicName: t.topicName,
            partitions: _.map(t.partitions, function (p) {
                return {
                    partition: p.partition,
                    error: error
                };
            })
        };
    });
}

Client.prototype.produceRequest = function (requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.protocol.write().ProduceRequest({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                requiredAcks: self.options.requiredAcks,
                timeout: self.options.timeout,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(buffer, self.options.requiredAcks === 0).then(function (responseBuffer) {
                if (self.options.requiredAcks !== 0) {
                    return self.protocol.read(responseBuffer).ProduceResponse().result.topics;
                }
            })
            .catch(errors.NoKafkaConnectionError, function (err) {
                return _fakeTopicsErrorResponse(topics, err);
            });
        }))
        .then(_mapTopics);
    });
};


Client.prototype.fetchRequest = function (requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer;
            // fake LeaderNotAvailable for all topics with no leader
            if (leader === -1 || !self.brokerConnections[leader]) {
                return _fakeTopicsErrorResponse(topics, errors.byName('LeaderNotAvailable'));
            }

            buffer = self.protocol.write().FetchRequest({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                timeout: self.options.timeout,
                minBytes: self.options.minBytes,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).FetchResponse().result.topics;
            })
            .catch(errors.NoKafkaConnectionError, function (err) {
                return _fakeTopicsErrorResponse(topics, err);
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetRequest = function (requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.protocol.write().OffsetRequest({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetCommitRequestV0 = function (groupId, requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.protocol.write().OffsetCommitRequestV0({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                groupId: groupId,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetCommitResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};


Client.prototype.offsetFetchRequestV0 = function (groupId, requests) {
    var self = this;

    return self._metadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer = self.protocol.write().OffsetFetchRequest({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                apiVersion: 0,
                groupId: groupId,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetFetchResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetFetchRequestV2 = function (groupId, requests) {
    var self = this;

    return self._metadata().then(function () {
        return self._findGroupCoordinator(groupId).then(function (connection) {
            var buffer = self.protocol.write().OffsetFetchRequest({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                apiVersion: 2,
                groupId: groupId,
                topics: requests
            }).result;

            return connection.send(buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetFetchResponse().result.topics;
            });
        })
        .then(_mapTopics);
    });
};

// close coordinator connection if 'NotCoordinatorForGroup' received
// not sure about 'GroupCoordinatorNotAvailable' or 'GroupLoadInProgress'..
Client.prototype.updateGroupCoordinator = function (groupId) {
    var self = this;
    if (self.groupCoordinators[groupId]) {
        return self.groupCoordinators[groupId].then(function (connection) {
            connection.close();
            delete self.groupCoordinators[groupId];
        });
    }
    return Promise.resolve();
};

Client.prototype._findGroupCoordinator = function (groupId) {
    var self = this, buffer;

    if (self.groupCoordinators[groupId] && !self.groupCoordinators[groupId].isRejected()) {
        return self.groupCoordinators[groupId];
    }

    buffer = self.protocol.write().GroupCoordinatorRequest({
        correlationId: self.correlationId++,
        clientId: self.options.clientId,
        groupId: groupId
    }).result;

    self.groupCoordinators[groupId] = Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).GroupCoordinatorResponse().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    }))
    .then(function (host) {
        return new Connection({
            host: host.coordinatorHost,
            port: host.coordinatorPort
        });
    })
    .catch(function (err) {
        if (err.length === 1) {
            throw err[0];
        }
        throw err;
    });

    return self.groupCoordinators[groupId];
};

Client.prototype.joinConsumerGroupRequest = function (groupId, memberId, sessionTimeout, strategies) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.protocol.write().JoinConsumerGroupRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId,
            groupId: groupId,
            sessionTimeout: sessionTimeout,
            memberId: memberId || '',
            groupProtocols: strategies
        }).result;

        return connection.send(buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).JoinConsumerGroupResponse().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.heartbeatRequest = function (groupId, memberId, generationId) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.protocol.write().HeartbeatRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId,
            generationId: generationId
        }).result;

        return connection.send(buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).HeartbeatResponse().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.syncConsumerGroupRequest = function (groupId, memberId, generationId, groupAssignment) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.protocol.write().SyncConsumerGroupRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId,
            generationId: generationId,
            groupAssignment: groupAssignment
        }).result;

        return connection.send(buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).SyncConsumerGroupResponse().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.leaveGroupRequest = function (groupId, memberId) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.protocol.write().LeaveGroupRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId
        }).result;

        return connection.send(buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).LeaveGroupResponse().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.offsetCommitRequestV2 = function (groupId, memberId, generationId, requests) {
    var self = this;

    return self._metadata().then(function () {
        return self._findGroupCoordinator(groupId).then(function (connection) {
            var buffer = self.protocol.write().OffsetCommitRequestV2({
                correlationId: self.correlationId++,
                clientId: self.options.clientId,
                groupId: groupId,
                generationId: generationId,
                memberId: memberId,
                retentionTime: self.options.retentionTime,
                topics: requests
            }).result;

            return connection.send(buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetCommitResponse().result.topics;
            });
        })
        .then(_mapTopics);
    });
};

Client.prototype.listGroupsRequest = function () {
    var self = this, buffer;

    return self._metadata().then(function () {
        buffer = self.protocol.write().ListGroupsRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId
        }).result;

        return Promise.map(_.values(self.brokerConnections), function (connection) {
            return connection.send(buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).ListGroupResponse().result.groups;
            });
        });
    }).then(_.flatten);
};

Client.prototype.describeGroupRequest = function (groupId) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.protocol.write().DescribeGroupRequest({
            correlationId: self.correlationId++,
            clientId: self.options.clientId,
            groups: [groupId]
        }).result;

        return connection.send(buffer).then(function (responseBuffer) {
            return self.protocol.read(responseBuffer).DescribeGroupResponse().result.groups[0];
        });
    });
};
