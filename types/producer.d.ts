
/// <reference path="./client.d.ts" />
/// <reference path="./index.d.ts" />

import Kafka = require("no-kafka");
import { Client } from "client";

declare module "producer" {

    export class Producer {
        constructor(options?: ProducerOptions);
        /**
         * Initializes the client for the producer.
         * 
         * @returns {Promise<Client>} 
         * 
         * @memberOf Producer
         */
        init(): Promise<Client>;
        /**
         * The send can take a single message or an array of messages.
         * It can have options
         * @memberOf Producer
         */
        send(data: Message | Message[], options?: SendOptions): Promise<Result[]>;
        /**
         * Close all connections.
         */
        end(): void;
    }

    export interface ProducerOptions {
        partitioner?: Partitioner;
        clientId?: string;
        asyncCompression?: boolean;
        codec?: Kafka.COMPRESSION_GZIP |
                Kafka.COMPRESSION_NONE |
                Kafka.COMPRESSION_SNAPPY;
    }

    export interface Partitioner {

    }
    export class DefaultPartitioner implements Partitioner {
        constructor();
    }

    export interface Result {
        topic: string;
        partition: number;
        offset: number;
    }

    export interface Message {
        topic: string;
        partition?: string;
        message: {
            key?: string;
            value: string;
            attributes?: string[];
        }
    }
    export interface SendOptions {
        /**
         * requiredAcks - require acknoledgments for produce request. 
         * If it is 0 the server will not send any response. 
         * If it is 1, the server will wait the data is 
         * written to the local log before sending a response. 
         * If it is -1 the server will block until the message 
         * is committed by all in sync replicas before sending a response. 
         * For any number > 1 the server will block waiting for 
         * this number of acknowledgements to occur 
         * (but the server will never wait for more acknowledgements 
         * than there are in-sync replicas).
         * default: 1
         */
        requiredAcks?: number;
        /** 
         * timeout - timeout in ms for produce request
         */
        timeout?: number;
        /**
         * clientId - ID of this client
         * default: 'no-kafka-client'
         */
        clientId?: string;
        /**
         * connectionString - comma delimited list of initial brokers list, 
         * default: '127.0.0.1:9092'
         */
        connectionString?: string;
        /**
         * reconnectionDelay - controls optionally progressive 
         * delay between reconnection attempts in case of network error:
         */
        reconnectionDelay?: {
            /**
             * min - minimum delay, used as increment value for next attempts.
             * default: 1000ms
             */
            min: number;
            /**
             * max - maximum delay value.
             * default: 1000ms
             */
            max: number;
        }
        /**
         * partitioner - Class instance used to determine topic partition for message.
         * If message already specifies a partition, the partitioner won't be used.
         * The partitioner must inherit from Kafka.DefaultPartitioner.
         * The partition method receives 3 arguments: the topic name,
         * an array with topic partitions, and the message (useful to partition by key, etc.).
         * partition can be sync or async (return a Promise).
         */
        partitioner?: any;
        /**
         * retries - controls number of attempts at delay 
         * between them when produce request fails
         */
        retries?: {

            /**
             * attempts - number of total attempts to send the message.
             * default: 3
             */
            attempts?: number;
            /**
             * delay - controls delay between retries,
             * the delay is progressive and incrememented
             * with each attempt with min value steps up to but not exceeding max value
             */
            delay?: {
                /**
                 * min - minimum delay, used as increment value for next attempts.
                 * default: 1000ms
                 */
                min?: number;
                /**
                 * max - maximum delay value.
                 * default: 3000ms
                 */
                max?: number;
            }
        }

        /**
         * codec - compression codec, one of 
         *   Kafka.COMPRESSION_NONE, Kafka.COMPRESSION_SNAPPY, Kafka.COMPRESSION_GZIP
         */
        codec: Kafka.COMPRESSION_NONE | Kafka.COMPRESSION_SNAPPY | Kafka.COMPRESSION_GZIP;
        /**
         * batch - control batching (grouping) of requests
         */
        batch?: {
            /**
             * size - group messages together into single 
             * batch until their total size exceeds this value.
             * default: 16384 bytes.
             * Set to 0 to disable batching.
             */
            size?: number;
            /**
             * maxWait - send grouped messages after this amount of 
             * milliseconds expire even if their total size 
             * doesn't exceed batch.size yet.
             * default: 10ms.
             * Set to 0 to disable batching.
             */
            maxWait?: number;
            /**
             * asyncCompression - boolean, 
             * use asynchronouse compression instead of synchronous.
             * default: false
             */
            asyncCompression?: boolean;
        }
    }
}
