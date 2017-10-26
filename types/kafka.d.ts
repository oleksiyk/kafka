// offset request time constants

export const EARLIEST_OFFSET = -2;
export const LATEST_OFFSET = -1;
export type OFFSET = -2 | -1;

// compression codecs
export const COMPRESSION_SNAPPY = 2;
export const COMPRESSION_GZIP = 1;
export const COMPRESSION_NONE = 0;
export type COMPRESSION = 0 | 1 | 2;

export interface Message {
    topic: string;
    partition?: number;
    offset?: number;
    message: {
        key?: string;
        value: Buffer | string;
        attributes?: string[];
    }
}


/**
  * A function returning a tuple of host (string) and port (integer), such as:

brokerRedirection: function (host, port) {
    return {
        host: host + ".somesuffix.com", // Fully qualify
        port: port + 100,               // Port NAT
    }
}
  */
export interface BrokerRedirectionFunction {
    (host: string, port: number): { host: string; port: number };
}

/**
  * A simple map of connection strings to new connection strings, such as:

brokerRedirection: {
    "some-host:9092": "actual-host:9092",
    "kafka://another-host:9092": "another-host:9093",
    "third-host:9092": "kafka://third-host:9000"
}
  */
export type BrokerRedirectionMap = {};


export interface AbstractAssignmentStrategy {
}

export { DefaultPartitioner } from "./assignment/partitioners/default";

export interface Logger {
    /**
      * 0 - nothing, 1 - just errors, 2 - +warnings, 
      * 3 - +info, 4 - +debug, 5 - +trace
      */
    logLevel?: 0 | 1 | 2 | 3 | 4 | 5;
    /**
      * logstash: {
        enabled: true,
        connectionString: "10.0.1.1:9999,10.0.1.2:9999",
        app: "myApp-kafka-consumer"
    }
      */
    logstash?: {
        enabled: boolean;
        connectionString: string;
        app: string;
    }
    /**
      * logFunction: console.log
      */
    logFunction?: any;
}

export interface Commit {
    topic?: string;
    partition?: number;
    offset?: number;
    metadata?: string;
}
