/// <reference path="producer.d.ts" />
/// <reference path="simple_consumer.d.ts" />
/// <reference path="group_consumer.d.ts" />

<<<<<<< HEAD
/// <reference path="./producer.d.ts" />

import { Producer as P } from "producer";

declare module "no-kafka" {
    export class Producer extends P { }
=======
/// <reference path="kafka.d.ts" />
/// <reference path="client.d.ts" />

declare module "no-kafka" {
>>>>>>> 014b43fa5b06d797f5ac801db41fb6fffa46eac0

    export * from "kafka";
    export { Producer } from "producer";
    export { SimpleConsumer } from "simple_consumer";
    export { GroupConsumer } from "group_consumer";

    // exports.GroupAdmin = require('./group_admin');

    // exports.DefaultPartitioner = require('./assignment/partitioners/default');
    // exports.HashCRC32Partitioner = require('./assignment/partitioners/hash_crc32');

    // exports.DefaultAssignmentStrategy = require('./assignment/strategies/default');
    // exports.ConsistentAssignmentStrategy = require('./assignment/strategies/consistent');
    // exports.WeightedRoundRobinAssignmentStrategy = require('./assignment/strategies/weighted_round_robin');

}
