import * as tls from "tls";
import * as Kafka from "./kafka";
import { Client } from "./client";

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
    send(data: Kafka.Message | Kafka.Message[], options?: SendOptions): Promise<Result[]>;
    /**
      * Close all connections.
      */
    end(): Promise<void>;
}

export interface ProducerOptions {
    /**
      * requiredAcks - require acknoledgments for produce request. 
      * If it is 0 the server will not send any response. 
      * If it is 1 (default), the server will wait the data 
      * is written to the local log before sending a response.
      * If it is -1 the server will block until the message is
      * committed by all in sync replicas before sending a response.
      * For any number > 1 the server will block waiting
      * for this number of acknowledgements to occur
      * (but the server will never wait for more acknowledgements
      * than there are in-sync replicas).
      * 
      * default: 1
      */
    requiredAcks?: number;
    /**
      * timeout - timeout in ms for produce request
      */
    timeout?: number;
    /**
      * clientId - ID of this client.
      * 
      * defaults to "no-kafka-client"
      */
    clientId?: string;
    /**
      * connectionString - comma delimited list of initial brokers list.
      * 
      * default: "127.0.0.1:9092"
      */
    connectionString?: string;
    /**
      * reconnectionDelay - controls optionally progressive 
      * delay between reconnection attempts in case of network error:
      */
    reconnectionDelay?: {
        /**
          * min - minimum delay, used as increment value for next attempts.
          * 
          * defaults to 1000ms
          */
        min?: number;
        /**
          * max - maximum delay value.
          * 
          * defaults to 1000ms
          */
        max?: number;
    }
    /**
      * partitioner - Class instance used to determine topic partition for message.
      * If message already specifies a partition,
      * the partitioner won't be used.
      * The partitioner must inherit from Kafka.DefaultPartitioner.
      */
    partitioner?: Kafka.DefaultPartitioner;
    /**
      * retries - controls number of attempts at delay 
      * between them when produce request fails
      */
    retries?: number;
    /**
      * attempts - number of total attempts to send the message.
      * 
      * defaults to 3
      */
    attempts?: number;
    /**
      * delay - controls delay between retries, 
      * the delay is progressive and incrememented 
      * with each attempt with min value steps up 
      * to but not exceeding max value
      */
    delay?: {
        /**
          * min - minimum delay, used as increment value for next attempts.
          * 
          * defaults to 1000ms
          */
        min?: number;
        /**
          * max - maximum delay value.
          * 
          * defaults to 3000ms
          */
        max?: number;
    }

    /**
      * codec - compression codec.
      */
    codec?: Kafka.COMPRESSION;
    /**
      * batch - control batching (grouping) of requests
      */
    batch?: {
        /**
          * size - group messages together into single batch 
          * until their total size exceeds this value.
          * Set to 0 to disable batching.
          * 
          * defaults to 16384 bytes.
          */
        size?: number;
        /**
          * maxWait - send grouped messages after this
          * amount of milliseconds expire even if their
          * total size doesn't exceed batch.size yet.
          * Set to 0 to disable batching.
          * 
          * defaults to 10ms.
          */
        maxWait?: number;
    }
    /**
      * asyncCompression - boolean, use asynchronouse 
      * compression instead of synchronous.
      * 
      * defaults to false
      */
    asyncCompression?: boolean;

    /**
      * To connect to Kafka with SSL endpoint enabled 
      * specify SSL certificate and key options to 
      * load cert/key from files or provide certificate/key 
      * directly as strings.
      * 
      * Should match `listeners` SSL option in Kafka config
      */
    ssl?: tls.ConnectionOptions;
    /**
      * connectionTimeout - timeout for establishing connection to Kafka in milliseconds
      * 
      * defaults to 3000ms
      */
    connectionTimeout?: number
    /**
      * socketTimeout - timeout for Kafka connection socket in milliseconds
      * 
      * defaults to 0 (disabled)
      */
    socketTimeout?: number

    brokerRedirection?: Kafka.BrokerRedirectionFunction | Kafka.BrokerRedirectionMap;

    logger?: Kafka.Logger;
}


export interface Result {
    topic: string;
    partition: number;
    offset: number;
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
    codec?: number; // Kafka.COMPRESSION_NONE | Kafka.COMPRESSION_SNAPPY | Kafka.COMPRESSION_GZIP;
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
