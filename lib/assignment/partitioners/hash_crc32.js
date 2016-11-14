'use strict';

var crc32    = require('../../protocol/misc/crc32');
var util     = require('util');
var DefaultPartitioner = require('./default');

function HashCRC32Partitioner() {
    DefaultPartitioner.apply(this, arguments);
}

util.inherits(HashCRC32Partitioner, DefaultPartitioner);

HashCRC32Partitioner.prototype.partition = function (_topicName, partitions, message) {
    var crc;

    if (message.key === undefined || message.key === null) {
        return 0;
    }

    crc = crc32(message.key);
    return crc % partitions.length;
};

module.exports = HashCRC32Partitioner;
