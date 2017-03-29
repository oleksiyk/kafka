/// <reference path="producer.d.ts" />
/// <reference path="simple_consumer.d.ts" />
/// <reference path="group_consumer.d.ts" />
/// <reference path="group_admin.d.ts" />

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
    export { Producer, Result } from "producer";
    export { SimpleConsumer } from "simple_consumer";
    export { GroupConsumer } from "group_consumer";

    export { GroupAdmin } from "group_admin";

    export { DefaultPartitioner } from "assignment/partitioners/default";
    export { HashCRC32Partitioner } from "assignment/partitioners/hash_crc32";

    export { DefaultAssignmentStrategy } from "assignment/strategies/default";
    export { ConsistentAssignmentStrategy } from "assignment/strategies/consistent";
    export { WeightedRoundRobinAssignmentStrategy } from "assignment/strategies/weighted_round_robin";

}