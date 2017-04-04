

declare module "assignment/strategies/weighted_round_robin" {
    import * as Kafka from "kafka";

    /** 
    * WeightedRoundRobinAssignmentStrategy weighted round robin assignment (based on wrr-pool).
    */
    export class WeightedRoundRobinAssignmentStrategy implements Kafka.AbstractAssignmentStrategy {
        constructor();
    }

}