import * as tls from 'tls';
/**
  * Something to hold the socket endpoint.
  * 
  * @export
  * @class Client
  */
export interface EndPoint {
    host: string;
    port: number;
}

export interface UpdateMetadata {
    isPending(): boolean;
}
/**
  * 
  * 
  * @export
  * @class Client
  */
export class Client {
    options: ClientOptions;
    finished: boolean;
    constructor(options?: ClientOptions);
    init(): Promise<Client>;
    end(): void;
    parseHostString(hostString: string): Promise<any[]>;
    checkBrokerRedirect(host: string, port: number): Promise<EndPoint>;
    nextCorrelationId(): number;
    updateMetadata(): UpdateMetadata;
    metadataRequest(topicNames: string[]): Promise<any>;
    getTopicPartitions(topic: string): Promise<any>;
    findLeader(topic: string, partition: number, notfoundOk: boolean): Promise<number>;
    leaderServer(leader: string): Promise<any>;
    produceRequest(requests: string[], codec: string): Promise<any>;
    fetchRequest(requests: string[]): Promise<any>;
    getPartitionOffset(leader: string, topic: string, partition: number, time: number): Promise<any>;
    offsetRequest(requests: string[]): Promise<any>;
    updateGroupCoordinator(groupid: string): Promise<any>;
    joinConsumerGroupRequest(groupid: string, memberid: any, sessionTimeout: number, strategies: any[]): Promise<any>;
    heartbeatRequest(groupId: any, memberId: any, generationId: any): Promise<any>;
    syncConsumerGroupRequest(groupId: any, memberId: any, generationId: any, groupAssignment: any): Promise<any>;
    leaveGroupRequest(groupId: any, memberId: any): Promise<any>;
    offsetCommitRequestV2(groupId: any, memberId: any, generationId: any, requests: any): Promise<any>;

    listGroupsRequest(): Promise<any>;
    describeGroupRequest(groupId: any): Promise<any>;

    log(...args: any[]): void;
    debug(...args: any[]): void;
    error(...args: any[]): void;
    warn(...args: any[]): void;
    trace(...args: any[]): void;
}

export interface ClientOptions {
    clientId?: string;
    connectionString?: string;
    ssl?: tls.ConnectionOptions;
    asyncCompression?: boolean;
    brokerRedirection?: boolean;
    reconnectionDelay?: {
        max?: number;
        min?: number;
    }
    logger?: {
        logLevel?: number;
        logstash?: {
            enabled?: boolean;
        }
    }
}
