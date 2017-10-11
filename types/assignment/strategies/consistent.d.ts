import * as Kafka from "../../kafka";

/** 
* ConsistentAssignmentStrategy which is based on a consistent hash ring and so provides consistent assignment across consumers in a group based on supplied metadata.id and metadata.weight options.
*/
export class ConsistentAssignmentStrategy implements Kafka.AbstractAssignmentStrategy {
    constructor();
}
