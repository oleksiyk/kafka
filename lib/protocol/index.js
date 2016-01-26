'use strict';

var Protocol = require('bin-protocol');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

var KafkaProtocol = Protocol.createProtocol();

module.exports = KafkaProtocol;

[
    'common',
    'metadata',
    'produce',
    'fetch',
    'offset',
    'offset_commit_fetch',
    'group_membership',
    'admin'
].forEach(function (m) {
    require('./' + m);
});
