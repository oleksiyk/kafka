'use strict';

var Promise     = require('./bluebird-configured');
var Connection  = require('./connection');
var Protocol    = require('./protocol');
var errors      = require('./errors');
var _           = require('lodash');
var Logger      = require('nice-simple-logger');
var compression = require('./protocol/misc/compression');
var url         = require('url');

function Client(options) {
    var self = this, logger;

    self.options = _.defaultsDeep(options || {}, {
        clientId: 'no-kafka-client',
        connectionString: process.env.KAFKA_URL || 'kafka://127.0.0.1:9092',
        asyncCompression: false,
        brokerRedirection: false,
        reconnectionDelay: {
            min: 1000,
            max: 1000
        },
        logger: {
            logLevel: 5,
            logstash: {
                enabled: false
            }
        }
    });

    logger = new Logger(self.options.logger);

    // prepend clientId argument
    ['log', 'debug', 'error', 'warn', 'trace'].forEach(function (m) {
        self[m] = _.bind(logger[m], logger, self.options.clientId);
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

    self._updateMetadata_running = Promise.resolve();
}

module.exports = Client;

function _mapTopics(topics) {
    return _(topics).flatten().transform(function (a, tv) {
        if (tv === null) { return; } // requiredAcks=0
        _.each(tv.partitions, function (p) {
            a.push(_.merge({
                topic: tv.topicName,
                partition: p.partition
            },
             _.omit(p, 'partition')
            /*function (_a, b) {if (b instanceof Buffer) {return b;}}*/) // fix for lodash _merge in Node v4: https://github.com/lodash/lodash/issues/1453
            );
        });
        return;
    }, []).value();
}

Client.prototype.init = function () {
    var self = this;

    self.initialBrokers = self.options.connectionString.split(',').map(function (hostStr) {
        var parsed, config;

        parsed = self.parseHostString(hostStr);
        config = self.checkBrokerRedirect(parsed.host, parsed.port);

        return config.host && config.port ? new Connection(config) : undefined;
    });

    self.initialBrokers = _.compact(self.initialBrokers);

    if (self.initialBrokers.length === 0) {
        return Promise.reject(new Error('No initial hosts to connect'));
    }

    return self.updateMetadata();
};

Client.prototype.end = function () {
    var self = this;

    self.finished = true;

    return Promise.map(
        Array.prototype.concat(self.initialBrokers, _.values(self.brokerConnections), _.values(self.groupCoordinators)),
        function (c) {
            return c.close();
        });
};

Client.prototype.parseHostString = function (hostString) {
    var hostStr = hostString.trim(), parsed;

    // Prepend the protocol, if required
    if (!/^([a-z]+:)?\/\//.test(hostStr)) {
        hostStr = 'kafka://' + hostStr;
    }
    parsed = url.parse(hostStr);

    return {
        host: parsed.hostname,
        port: parseInt(parsed.port)
    };
};

Client.prototype.checkBrokerRedirect = function (host, port) {
    var fullName, fullNameProtocol;
    var redirect = this.options.brokerRedirection;

    // No remapper
    if (!redirect) {
        return {
            host: host,
            port: port
        };
    }

    // Use a function
    if (typeof redirect === 'function') {
        return redirect(host, port);
    }

    // Name, without protocol
    fullName = host + ':' + port.toString();
    if (redirect[fullName]) {
        return this.parseHostString(redirect[fullName]);
    }

    // Name, with protocol
    fullNameProtocol = 'kafka://' + host + ':' + port.toString();
    if (redirect[fullNameProtocol]) {
        return this.parseHostString(redirect[fullNameProtocol]);
    }

    return {
        host: host,
        port: port
    };
};

Client.prototype.nextCorrelationId = function () {
    if (this.correlationId === 2147483647) {
        this.correlationId = 0;
    }
    return this.correlationId++;
};

Client.prototype.updateMetadata = function () {
    var self = this, attempt = 1;

    if (self._updateMetadata_running.isPending()) {
        return self._updateMetadata_running;
    }

    self._updateMetadata_running = (function _try() {
        if (self.finished === true) {
            return null;
        }

        return self.metadataRequest().then(function (response) {
            var oldConnections = self.brokerConnections;

            self.brokerConnections = {};

            _.each(response.broker, function (broker) {
                var remapped = self.checkBrokerRedirect(broker.host, broker.port);
                var connection = _.find(oldConnections, function (c, i) {
                    return c.equal(remapped.host, remapped.port) && delete oldConnections[i];
                });

                self.brokerConnections[broker.nodeId] = connection || new Connection({
                    host: remapped.host,
                    port: remapped.port
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
                return Promise.delay(1000).then(_try);
            }

            return null;
        })
        .catch(function (err) {
            var delay = _.min([attempt++ * self.options.reconnectionDelay.min, self.options.reconnectionDelay.max]);
            self.error('Metadata request failed:', err);
            return Promise.delay(delay).then(_try);
        });
    }());

    return self._updateMetadata_running;
};

Client.prototype._waitMetadata = function () {
    var self = this;

    if (self._updateMetadata_running.isPending()) {
        return self._updateMetadata_running;
    }

    return Promise.resolve();
};

Client.prototype.metadataRequest = function (topicNames) {
    var self = this, buffer, correlationId = self.nextCorrelationId();

    buffer = self.protocol.write().MetadataRequest({
        correlationId: correlationId,
        clientId: self.options.clientId,
        topicNames: topicNames || []
    }).result;

    return Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(correlationId, buffer).then(function (responseBuffer) {
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

Client.prototype.getTopicPartitions = function (topic) {
    var self = this;

    function _try() {
        if (self.topicMetadata.hasOwnProperty(topic)) {
            return self.topicMetadata[topic];
        }
        throw errors.byName('UnknownTopicOrPartition');
    }

    return self._waitMetadata().then(_try)
    .catch({ code: 'UnknownTopicOrPartition' }, function () {
        return self.updateMetadata().then(_try);
    })
    .then(_.values);
};

Client.prototype.findLeader = function (topic, partition, notfoundOK) {
    var self = this;

    function _try() {
        var r = _.get(self.topicMetadata, [topic, partition, 'leader'], notfoundOK ? parseInt(_.keys(self.brokerConnections)[0]) : -1);
        if (r === -1) {
            throw errors.byName('UnknownTopicOrPartition');
        }
        if (!self.brokerConnections[r]) {
            throw errors.byName('LeaderNotAvailable');
        }
        return r;
    }

    return self._waitMetadata().then(_try)
    .catch({ code: 'UnknownTopicOrPartition' }, { code: 'LeaderNotAvailable' }, function () {
        return self.updateMetadata().then(_try);
    });
};

Client.prototype.leaderServer = function (leader) {
    return _.result(this.brokerConnections, [leader, 'server'], '-');
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

Client.prototype.produceRequest = function (requests, codec) {
    var self = this, compressionPromises = [];

    function processPartition(pv, pk) {
        var _r = {
            partition: parseInt(pk),
            messageSet: []
        };
        compressionPromises.push(self._compressMessageSet(_.map(pv, function (mv) {
            return { offset: 0, message: mv.message };
        }), codec).then(function (messageSet) {
            _r.messageSet = messageSet;
        }));
        return _r;
    }

    return self._waitMetadata().then(function () {
        requests = _(requests).groupBy('leader').mapValues(function (v) {
            return _(v)
                .groupBy('topic')
                .map(function (p, t) {
                    return {
                        topicName: t,
                        partitions: _(p).groupBy('partition').map(processPartition).value()
                    };
                })
                .value();
        }).value();

        return Promise.all(compressionPromises).then(function () {
            return Promise.all(_.map(requests, function (topics, leader) {
                var correlationId = self.nextCorrelationId();
                var buffer = self.protocol.write().ProduceRequest({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    requiredAcks: self.options.requiredAcks,
                    timeout: self.options.timeout,
                    topics: topics
                }).result;

                return self.brokerConnections[leader].send(correlationId, buffer, self.options.requiredAcks === 0).then(function (responseBuffer) {
                    if (self.options.requiredAcks !== 0) {
                        // TODO: ThrottleTime is returned in V1 so we should change the return value soon
                        // [ topics, throttleTime ] or { topics, throttleTime }
                        // first one will allow to just use .spread instead of .then
                        // second will be more generic but probably require more changes to the user code
                        return self.protocol.read(responseBuffer).ProduceResponse().result.topics;
                    }
                    return null;
                })
                .catch(errors.NoKafkaConnectionError, function (err) {
                    return _fakeTopicsErrorResponse(topics, err);
                });
            }));
        })
        .then(_mapTopics);
    });
};

Client.prototype.fetchRequest = function (requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer, correlationId = self.nextCorrelationId();
            // fake LeaderNotAvailable for all topics with no leader
            if (leader === -1 || !self.brokerConnections[leader]) {
                return _fakeTopicsErrorResponse(topics, errors.byName('LeaderNotAvailable'));
            }

            buffer = self.protocol.write().FetchRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                maxWaitTime: self.options.maxWaitTime,
                minBytes: self.options.minBytes,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(correlationId, buffer).then(function (responseBuffer) {
                // TODO: ThrottleTime is returned in V1 so we should change the return value soon
                // [ topics, throttleTime ] or { topics, throttleTime }
                // first one will allow to just use .spread instead of .then
                // second will be more generic but probably require more changes to the user code
                return self.protocol.read(responseBuffer).FetchResponse().result.topics;
            })
            .catch(errors.NoKafkaConnectionError, function (err) {
                return _fakeTopicsErrorResponse(topics, err);
            });
        }))
        .then(function (topics) {
            return Promise.map(_mapTopics(topics), function (r) {
                return Promise.map(r.messageSet || [], function (m) {
                    if (m.message.attributes.codec === 0) {
                        return Promise.resolve(m);
                    }

                    return self._decompressMessageSet(m.message).catch(function (err) {
                        self.error('Failed to decompress message at', r.topic + ':' + r.partition + '@' + m.offset, err);
                    });
                })
                .then(function (newMessageSet) {
                    return _.assign({}, r, { messageSet: _.flatten(newMessageSet) });
                });
            });
        });
    });
};

Client.prototype.offsetRequest = function (requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var correlationId = self.nextCorrelationId();
            var buffer = self.protocol.write().OffsetRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(correlationId, buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetCommitRequestV0 = function (groupId, requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var correlationId = self.nextCorrelationId();
            var buffer = self.protocol.write().OffsetCommitRequestV0({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(correlationId, buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetCommitResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};


Client.prototype.offsetFetchRequestV0 = function (groupId, requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(_.map(requests, function (topics, leader) {
            var correlationId = self.nextCorrelationId();
            var buffer = self.protocol.write().OffsetFetchRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: 0,
                groupId: groupId,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(correlationId, buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetFetchResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetFetchRequestV1 = function (groupId, requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return self._findGroupCoordinator(groupId).then(function (connection) {
            var correlationId = self.nextCorrelationId();
            var buffer = self.protocol.write().OffsetFetchRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: 1,
                groupId: groupId,
                topics: requests
            }).result;

            return connection.send(correlationId, buffer).then(function (responseBuffer) {
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
    if (self.groupCoordinators[groupId] && !self.groupCoordinators[groupId].isRejected()) {
        return self.groupCoordinators[groupId].then(function (connection) {
            connection.close();
            delete self.groupCoordinators[groupId];
        });
    }
    delete self.groupCoordinators[groupId];
    return Promise.resolve();
};

Client.prototype._findGroupCoordinator = function (groupId) {
    var self = this, buffer, correlationId = self.nextCorrelationId();

    if (self.groupCoordinators[groupId] && !self.groupCoordinators[groupId].isRejected()) {
        return self.groupCoordinators[groupId];
    }

    buffer = self.protocol.write().GroupCoordinatorRequest({
        correlationId: correlationId,
        clientId: self.options.clientId,
        groupId: groupId
    }).result;

    self.groupCoordinators[groupId] = Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).GroupCoordinatorResponse().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    }))
    .then(function (host) {
        var remapped = self.checkBrokerRedirect(host.coordinatorHost, host.coordinatorPort);
        return new Connection(remapped);
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
        var correlationId = self.nextCorrelationId();
        var buffer = self.protocol.write().JoinConsumerGroupRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            groupId: groupId,
            sessionTimeout: sessionTimeout,
            memberId: memberId || '',
            groupProtocols: strategies
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
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
        var correlationId = self.nextCorrelationId();
        var buffer = self.protocol.write().HeartbeatRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId,
            generationId: generationId
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
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
        var correlationId = self.nextCorrelationId();
        var buffer = self.protocol.write().SyncConsumerGroupRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId,
            generationId: generationId,
            groupAssignment: groupAssignment
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
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
        var correlationId = self.nextCorrelationId();
        var buffer = self.protocol.write().LeaveGroupRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
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

    return self._waitMetadata().then(function () {
        return self._findGroupCoordinator(groupId).then(function (connection) {
            var correlationId = self.nextCorrelationId();
            var buffer = self.protocol.write().OffsetCommitRequestV2({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                generationId: generationId,
                memberId: memberId,
                retentionTime: self.options.retentionTime,
                topics: requests
            }).result;

            return connection.send(correlationId, buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetCommitResponse().result.topics;
            });
        })
        .then(_mapTopics);
    });
};

Client.prototype.listGroupsRequest = function () {
    var self = this, buffer, correlationId = self.nextCorrelationId();

    return self._waitMetadata().then(function () {
        buffer = self.protocol.write().ListGroupsRequest({
            correlationId: correlationId,
            clientId: self.options.clientId
        }).result;

        return Promise.map(_.values(self.brokerConnections), function (connection) {
            return connection.send(correlationId, buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).ListGroupResponse().result.groups;
            });
        });
    }).then(_.flatten);
};

Client.prototype.describeGroupRequest = function (groupId) {
    var self = this, correlationId = self.nextCorrelationId();

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var buffer = self.protocol.write().DescribeGroupRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            groups: [groupId]
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            return self.protocol.read(responseBuffer).DescribeGroupResponse().result.groups[0];
        });
    });
};

Client.prototype._decompressMessageSet = function (message) {
    var self = this;
    var decompress = self.options.asyncCompression ? compression.decompressAsync : compression.decompress;

    return decompress(message.value, message.attributes.codec).then(function (buffer) {
        return self.protocol.read(buffer).MessageSet(null, buffer.length).result;
    });
};

Client.prototype._compressMessageSet = function (messageSet, codec) {
    var self = this, buffer;
    var compress = self.options.asyncCompression ? compression.compressAsync : compression.compress;

    if (codec === 0) { return Promise.resolve(messageSet); }
    buffer = self.protocol.write().MessageSet(messageSet).result;

    return compress(buffer, codec).then(function (_buffer) {
        return [{
            offset: 0,
            message: {
                value: _buffer,
                attributes: {
                    codec: codec
                }
            }
        }];
    })
    .catch(function (err) {
        self.warn('Failed to compress messageSet', err);
        return messageSet;
    });
};
