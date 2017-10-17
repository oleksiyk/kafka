import * as tls from 'tls';
import * as Kafka from "./kafka";
import { BaseConsumer } from "./base_consumer";

export class SimpleConsumer extends BaseConsumer {
    constructor(options?: SimpleConsumerOptions);
    commitOffset(commits: Kafka.Commit[]): Promise<any>;
    fetchOffset(commits: Kafka.Commit | Kafka.Commit[]): Promise<number[]>;  
}

export interface SimpleConsumerOptions {
    /**
      * `groupId` - group ID for comitting and 
      * fetching offsets. 
      * Default: "no-kafka-group-v0"
      */
    groupId?: string;
    /**
      * `maxWaitTime` - maximum amount of time in 
      * milliseconds to block waiting if insufficient 
      * data is available at the time the fetch request is issued.
      * 
      * default: 100ms
      */
    maxWaitTime?: number;
    /**
      * `idleTimeout` - timeout between fetch calls.
      * 
      * default: 1000ms
      */
    idleTimeout?: number;
    /**
      * `minBytes` - minimum number of bytes to 
      * wait from Kafka before returning the fetch call.
      * 
      * default: 1 byte
      */
    minBytes?: number;
    /**
      * `maxBytes` - maximum size of messages in a fetch response.
      * 
      * default: to 1MB
      */
    maxBytes?: number;
    /**
      * `clientId` - ID of this client.
      * 
      * default: "no-kafka-client"
      */
    clientId?: string;
    /**
      * `connectionString` - comma delimited list of initial brokers list.
      * 
      * default: "127.0.0.1:9092"
      */
    connectionString?: string;
    /**
      * `reconnectionDelay` - controls optionally progressive 
      * delay between reconnection attempts in case of network error:
      */
    reconnectionDelay?: {
        /**
          * `min` - minimum delay, used as increment value 
          * for next attempts.
          * 
          * defaults to 1000ms
          */
        min?: number;
        /**
          * `max` - maximum delay value.
          * 
          * defaults to 1000ms
          */
        max?: number;
    }
    /**
      * `recoveryOffset` - recovery position (time) which will used 
      * to recover subscription in case of OffsetOutOfRange error.
      * 
      * defaults to Kafka.LATEST_OFFSET
      */
    recoveryOffset?: number;
    /**
      * `asyncCompression` - boolean, use asynchronouse decompression 
      * instead of synchronous.
      * 
      * defaults to `false`
      */
    asyncCompression?: boolean;
    /**
      * `handlerConcurrency` - specify concurrency level 
      * for the consumer handler function.
      * 
      * default: 10
      */
    handlerConcurrency?: number;
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

    ssl?: tls.ConnectionOptions;

    logger?: Kafka.Logger;
}
