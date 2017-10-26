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
