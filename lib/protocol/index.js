'use strict';

var protocol = require('bin-protocol');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

module.exports = protocol;

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
