
/// <reference path="./client.d.ts" />
/// <reference path="./index.d.ts" />

import Kafka = require("no-kafka");
import { Client } from "client";

declare module "consumer" {

    export class BaseConsumer {
        constructor(options: ConsumerOptions);

        init(): Promise<void>;

        subscribe(topic: string, offset: number | number[],
            options: ConsumerOptions,
            handler: DataHandler): Promise<void>;

        unsubscribe(topic: string, partitions: number | number[]): Promise<number[]>;
        offset(topic: string, partition?: number): Promise<number>;

        end(): Promise<void>;

    }

    export interface ConsumerOptions {
        offset?: number;
        maxBytes: number;
        time?: Kafka.EARLIEST_OFFSET | Kafka.LATEST_OFFSET
    }

    export interface DataHandler {
        (messageSet: Message[],
            topic: string,
            partition?: number): Promise<any>;
    }

}