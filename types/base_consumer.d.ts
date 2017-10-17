import * as Kafka from "./kafka";

export class BaseConsumer {
    constructor(options: BaseConsumerOptions);

    init(): Promise<void>;

    subscribe(topic: string, partitions: number | number[],
        options: BaseConsumerOptions,
        handler: DataHandler): Promise<void>;

    unsubscribe(topic: string, partitions?: number | number[]): Promise<number[]>;
    offset(topic: string, partition?: number): Promise<number>;

    end(): Promise<void>;
}

export interface BaseConsumerOptions {
    offset?: number;
    maxBytes?: number;
    time?: Kafka.OFFSET
}

export interface DataHandler {
    (messageSet: Kafka.Message[],
        topic: string,
        partition?: number): Promise<any>;
}

