
/// <reference path="./producer.d.ts" />

import {Producer as P} from  "producer";

declare module "no-kafka" {
    export class Producer extends P {}

    // exports.SimpleConsumer = require('./simple_consumer');
    // exports.GroupConsumer = require('./group_consumer');
    // exports.GroupAdmin = require('./group_admin');

    // exports.DefaultPartitioner = require('./assignment/partitioners/default');
    // exports.HashCRC32Partitioner = require('./assignment/partitioners/hash_crc32');

    // exports.DefaultAssignmentStrategy = require('./assignment/strategies/default');
    // exports.ConsistentAssignmentStrategy = require('./assignment/strategies/consistent');
    // exports.WeightedRoundRobinAssignmentStrategy = require('./assignment/strategies/weighted_round_robin');

    // offset request time constants
    export const EARLIEST_OFFSET = -2;
    export const LATEST_OFFSET = -1;

    // compression codecs
    export const COMPRESSION_SNAPPY = 2;
    export const COMPRESSION_GZIP = 1;
    export const COMPRESSION_NONE = 0;
}
