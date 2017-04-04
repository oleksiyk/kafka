/// <reference path="producer.d.ts" />
/// <reference path="base_consumer.d.ts" />
/// <reference path="simple_consumer.d.ts" />
/// <reference path="group_consumer.d.ts" />
/// <reference path="group_admin.d.ts" />

/// <reference path="kafka.d.ts" />
/// <reference path="client.d.ts" />

/// <reference path="assignment/partitioners/default.d.ts" />
/// <reference path="assignment/partitioners/hash_crc32.d.ts" />
/// <reference path="assignment/strategies/default.d.ts" />
/// <reference path="assignment/strategies/consistent.d.ts" />
/// <reference path="assignment/strategies/weighted_round_robin.d.ts" />

declare module "no-kafka" {

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