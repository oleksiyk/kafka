

declare module "kafka" {

    // offset request time constants

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
        message: {
            key?: string;
            value: string;
            attributes?: string[];
        }
    }


    export class DefaultPartitioner {
        constructor();
        /**
         * The partition method receives 3 arguments: 
         * * the topic name, 
         * * an array with topic partitions, and 
         * * the message (useful to partition by key, etc.). 
         * The call to partition can be sync or async (return a Promise).
         */
        partition(topic: string, partitions: number[], message: string): Promise<number>;

    }
    /**
     * 
     * 
     * @export
     * @class AbstractAssignmentStrategy
     */
    export class AbstractAssignmentStrategy {
        constructor();
    }
    /**
     * simple round robin assignment strategy (default).
     * 
     * @export
     * @class DefaultAssignmentStrategy
     */
    export class DefaultAssignmentStrategy extends AbstractAssignmentStrategy {
        constructor();
    }
    /** 
    * WeightedRoundRobinAssignmentStrategy weighted round robin assignment (based on wrr-pool).
    */
    export class WeightedRoundRobinAssignmentStrategy extends AbstractAssignmentStrategy {
        constructor();
    }
    /** 
    * ConsistentAssignmentStrategy which is based on a consistent hash ring and so provides consistent assignment across consumers in a group based on supplied metadata.id and metadata.weight options.
    */
    export class ConsistentAssignmentStrategy extends AbstractAssignmentStrategy {
        constructor();
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

}
