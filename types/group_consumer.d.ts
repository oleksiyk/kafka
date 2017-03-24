/// <reference path="./base_consumer.d.ts" />
/// <reference path="./index.d.ts" />

import Kafka = require("no-kafka");
import { Client } from "client";
import { BaseConsumer } from "base_consumer";

declare module "group_consumer" {

    export class GroupConsumer extends BaseConsumer {
        commitOffset(commits: Commit[]): Promise<any>;
        fetchOffset(commits): Promise<number[]>;
        
    }
}