import { Client } from "./client";

export class GroupAdmin {
    constructor();

    /**
      * list existing consumer groups
      */
    listGroups(): Group[];
    /**
      * describeGroup - describe existing group by its id
      */
    describeGroup(groupId: string): GroupDescription;
    /**
      * fetchConsumerLag - fetches consumer lag for topics/partitions
      */
    fetchConsumerLag(groupId: string, options: FetchOptions): ConsumerLag[];

}

interface Group {
    groupId?: string;
    protocolType?: string;
}

interface GroupDescription {
    error?: null | any;
    groupId?: string;
    state?: string; // Stable
    protocolType?: string; // consumer
    members?: MemberDescription[];

}

interface MemberDescription {
    memberId: string;
    clientId: string;
    clientHost: string;
    version: number;
    subscriptions: string[];
    metadata: any;
    memberAssignment: {
        _blength: number;
        version: number;
        partitionAssignment: PartitionAssignment[];
        metadata: null | any;
    }
}
export interface PartitionAssignment {
    topic?: string;
    partitions?: number[];
}

export interface FetchOptions {
    topicName?: string;
    partitions?: number[];
}

/**
* Note that group consumer has to commit offsets first, in order for consumerLag to be available. 
* Otherwise the offset will be set to -1.
*/
export interface ConsumerLag {
    topic: string;
    partition: number;
    offset: number;
    highwaterMark: number;
    consumerLag: number | null;
}
