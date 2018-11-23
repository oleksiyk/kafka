'use strict';

var Docker = require('dockerode');
var Promise = require('bluebird');
var { pullImageAsync, imageExists, containerExec, waitForOutput } = require('dockerode-utils');

var docker = new Docker();
var container = {};
var dockerKafkaPort = 9092;
var dockerZookeeperPort = 2181;

function getConnectionString() {
    return `kafka://127.0.0.1:${dockerKafkaPort}`;
}

function createTopic(topicName) {
    var kafkaCommand = [
        '$KAFKA_HOME/bin/kafka-topics.sh',
        '--zookeeper', '127.0.0.1:2181',
        '--create', '--topic', topicName,
        '--partitions', '3',
        '--replication-factor', '1',
    ];
    var command = [
        'bash', '-c', kafkaCommand.join(' '),
    ];
    return containerExec(container, command);
}

function createTopics(topicNames) {
    return Promise.map(topicNames, function (topicName) {
        return createTopic(topicName);
    });
}

before(function () {
    this.timeout(120000);
    return imageExists(docker, 'spotify/kafka')
    .then(function (exists) {
        if (!exists) {
            return pullImageAsync(docker, 'spotify/kafka');
        }
        return Promise.resolve();
    })
    .then(function () {
        return docker.createContainer({
            Image: 'spotify/kafka',
            Env: [
                'ADVERTISED_HOST=localhost',
                `ADVERTISED_PORT=${dockerKafkaPort}`,
            ],
            HostConfig: {
                PortBindings: {
                    ['2181/tcp']: [{ HostPort: `${dockerZookeeperPort}/tcp` }],
                    ['9092/tcp']: [{ HostPort: `${dockerKafkaPort}/tcp` }],
                },
            }
        });
    }).then(function (_container) {
        container = _container;
        return container.start();
    })
    .then(function () {
        console.log('Waiting for kafka to start...'); // eslint-disable-line
        return waitForOutput(container, function (line) {
            return line.search('kafka entered RUNNING state') > 0;
        });
    }).then(function () {
        console.log('Kafka started'); // eslint-disable-line
    });
});

after(function () {
    return docker.listContainers().then(function (containers) {
        return containers.filter(function (_container) { return _container.Image === 'spotify/kafka'; });
    }).then(function (containers) {
        return Promise.map(containers, function (_container) {
            return docker.getContainer(_container.Id).stop();
        });
    });
});

module.exports = {
    getConnectionString,
    createTopic,
    createTopics,
};
