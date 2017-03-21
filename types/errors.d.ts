
declare module "errors" {

    export interface KafkaErrorI {
        name: string;
        code: any;
        message: string;

    }
    export class KafkaError implements KafkaErrorI {
        constructor(code: any, message: string);

        toJSON(): KafkaErrorI;
        toString(): string;

    }
    export function byCode(code: any): null | KafkaError;
    export function byName(name: string): null | KafkaError;

    export interface NoKafkaConnectionErrorI {
        name: string;
        server: any;
        message: string;

    }
    export class NoKafkaConnectionError implements NoKafkaConnectionErrorI {
        constructor(server: any, message: string);

        toJSON(): NoKafkaConnectionErrorI;
        toString(): string;

    }

}