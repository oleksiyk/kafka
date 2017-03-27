


declare module "base_consumer" {
    import * as Kafka from "kafka";

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
        time?: Kafka.OFFSET
    }

    export interface DataHandler {
        (messageSet: Kafka.Message[],
            topic: string,
            partition?: number): Promise<any>;
    }

}