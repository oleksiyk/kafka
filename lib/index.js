'use strict';

module.exports = (function () {
    exports.Producer = require('./producer');
    exports.SimpleConsumer = require('./simple_consumer');
    exports.GroupConsumer = require('./group_consumer');
    exports.GroupAdmin = require('./group_admin');

    exports.DefaultPartitioner = require('./assignment/partitioners/default');
    exports.HashCRC32Partitioner = require('./assignment/partitioners/hash_crc32');

    exports.DefaultAssignmentStrategy = require('./assignment/strategies/default');
    exports.ConsistentAssignmentStrategy = require('./assignment/strategies/consistent');
    exports.WeightedRoundRobinAssignmentStrategy = require('./assignment/strategies/weighted_round_robin');

    // offset request time constants
    exports.EARLIEST_OFFSET = -2;
    exports.LATEST_OFFSET = -1;

    // compression codecs
    exports.COMPRESSION_SNAPPY = 2;
    exports.COMPRESSION_GZIP = 1;
    exports.COMPRESSION_NONE = 0;

    return exports;
}());
