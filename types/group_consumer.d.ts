


declare module "group_consumer" {
    import { BaseConsumer } from "base_consumer";

    export class GroupConsumer extends BaseConsumer {
        // commitOffset(commits: Commit[]): Promise<any>;
        fetchOffset(commits): Promise<number[]>;

    }
}