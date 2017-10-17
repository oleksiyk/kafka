import * as Kafka from "./kafka";
import { BaseConsumer } from "./base_consumer";

export class GroupConsumer extends BaseConsumer {
    constructor(options?: GroupConsumerOptions);

    commitOffset(commits: Kafka.Commit | Kafka.Commit[]): Promise<any>;
    fetchOffset(commits: Kafka.Commit | Kafka.Commit[]): Promise<Kafka.Commit[]>;

    init(strategies?: Strategy | Strategy[]): Promise<any>;
}

export interface GroupConsumerOptions {
    /**
      * groupId - group ID for comitting and fetching offsets.
      * 
      * Defaults to "no-kafka-group-v0.9"
      */
    groupId?: string;
    /**
      * maxWaitTime - maximum amount of time in milliseconds to 
      * block waiting if insufficient data is available at the 
      * time the fetch request is issued.
      * 
      * defaults to 100ms
      */
    maxWaitTime?: number;
    /**
      * idleTimeout - timeout between fetch calls.
      * 
      * defaults to 1000ms
      */
    idleTimeout?: number;
    /**
      * minBytes - minimum number of bytes to wait 
      * from Kafka before returning the fetch call.
      * 
      * defaults to 1 byte
      */
    minBytes?: number;
    /**
      * maxBytes - maximum size of messages in a fetch response
      */
    maxBytes?: number;
    /**
      * clientId - ID of this client.
      * 
      * defaults to "no-kafka-client"
      */
    clientId?: string;
    /**
      * connectionString - comma delimited list of initial brokers list.
      * 
      * defaults to "127.0.0.1:9092"
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
      * sessionTimeout - session timeout in ms, min 6000, max 30000.
      * 
      * defaults to 15000
      */
    sessionTimeout?: number;
    /**
      * heartbeatTimeout - delay between heartbeat requests in ms.
      * 
      * defaults to 1000
      */
    heartbeatTimeout?: number;
    /**
      * retentionTime - offset retention time in ms.
      * 
      * defaults to 1 day (24 * 3600 * 1000)
      */
    retentionTime?: number;
    /**
      * startingOffset - starting position (time) when there is no commited offset.
      * 
      * defaults to Kafka.LATEST_OFFSET
      */
    startingOffset?: Kafka.OFFSET;
    /**
      * recoveryOffset - recovery position (time) which will 
      * used to recover subscription in case of OffsetOutOfRange error.
      * 
      * defaults to Kafka.LATEST_OFFSET
      */
    recoveryOffset?: Kafka.OFFSET;
    /**
      * asyncCompression - boolean, use asynchronouse decompression 
      * instead of synchronous.
      * 
      * defaults to false
      */
    asyncCompression?: boolean;
    /**
      * handlerConcurrency - specify concurrency level for the 
      * consumer handler function.
      * 
      * defaults to 10
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

    logger?: Kafka.Logger;
}

export interface Strategy {
    subscriptions?: string[];
    handler?: DataHandler;
    metadata?: {
        id?: string;
        weight?: number;
    }
    strategy?: Kafka.AbstractAssignmentStrategy;
}


export interface DataHandler {
    (messageSet: Kafka.Message[], topic: string, partition: number): Promise<any>;
}
