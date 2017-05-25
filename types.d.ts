declare namespace Kafka {
  interface Message {
    topic: string;
    partition?: number;
    message: any;
  }

  interface PartitionInfo {
    error: string | null;
    partitionId: number;
    leader: number;
    replicas: number[];
    isr: number[];
  }

  interface ProducerOptions {
    requiredAcks?: number;
    timeout?: number;
    clientId?: string;
    connectionString?: string;
    reconnectionDelay?: number;
    partitioner?: (topic:string, partitions: PartitionInfo[], message:any) => number | Promise<number>;
    retries?: {
      attempts: number;
      delay: number;
    };
    codec?: string;
    batch?: {
      size: number;
      maxWait: number;
    };
    asyncCompression?: boolean;
  }

  interface SendOptions {
    codec?: string;
    retries?: {
      attempts: number;
      delay: number;
    };
    batch?: {
      size: number;
    };
  }

  interface BaseConsumerOptions {
    groupId?: string;
    maxWaitTime?: number;
    idleTimeout?: number;
    minBytes?: number;
    maxBytes?: number;
    clientId?: string;
    connectionString?: string;
    reconnectionDelay?: {
      min: number;
      max: number;
    };
    recoveryOffset?: string;
    asyncCompression?: boolean;
    handlerConcurrency?: number;
  }

  interface SimpleConsumerOptions extends BaseConsumerOptions {
  }

  interface GroupConsumerOptions extends BaseConsumerOptions {
    sessionTimeout?: number;
    heartbeatTimeout?: number;
    startingOffset?: string;
    retentionTime?: number;
  }

  interface DataHandler {
    (messageSet: any[], topic: string, partition: number): void | Promise<void>;
  }

  interface ConsumerStrategies {
    strategy: string;
    subscriptions: string[];
    metadata: any;
    fn: Function;
    handler: DataHandler;
  }

  interface CommitOffset {
    topic: string;
    partition: number;
    offset: any;
    metadata: any;
  }

  interface Commit {
    topic: string;
    partition: number;
  }

  interface Group {
    groupId: string;
  }

  interface GroupDescription {
    error: string | null;
    groupId: string;
    state: string;
    protocolType: string;
    protocol: string;
    members: {
      memberId: string;
      clientId: string;
      clientHost: string;
      version: number;
      subscriptions: string[];
      metadata: Buffer | null;
      memberAssignment: {
        _blength: number;
        version: number;
        partitionAssignment: {
          topic: string;
          partitions: number[];
        }[]
        metadata: Buffer | null
      }
    }[];
  }
}

declare module "no-kafka" {
  export class Producer {
    constructor(options?:Kafka.ProducerOptions);
    init():Promise<void>;
    send(data: Kafka.Message, options?: Kafka.SendOptions): Promise<any>
    send(data: Kafka.Message[], options?: Kafka.SendOptions): Promise<any>
    end():Promise<void>;
  }

  export class SimpleConsumer {
    constructor(options?:Kafka.SimpleConsumerOptions);
    init():Promise<void>;
    subscribe(topic: string, partitions: number[], dataHandler: Kafka.DataHandler);
    subscribe(topic: string, partition: number, offsets: {time?: string; offset?: number, maxBytes?:number}, dataHandler: Kafka.DataHandler);
    subscribe(topic: string, dataHandler: Kafka.DataHandler);
    commitOffset(commits:Kafka.CommitOffset | Kafka.CommitOffset[]):Promise<void>
    fetchOffset(commits:Kafka.Commit | Kafka.Commit[]): Promise<any>
  }

  export class GroupConsumer {
    constructor(options?:Kafka.GroupConsumerOptions);
    init(strategies:Kafka.ConsumerStrategies);
    end():Promise<void>;
    commitOffset(commits:Kafka.CommitOffset | Kafka.CommitOffset[]):Promise<void>
    fetchOffset(commits:Kafka.Commit | Kafka.Commit[]): Promise<any>
  }

  export class GroupAdmin {
    constructor();
    listGroups():Promise<Kafka.Group[]>;
    describeGroup():Promise<Kafka.GroupDescription>;
  }
}