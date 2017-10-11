export class KafkaErrorAbstract {
    name: string;
    code: any;
    message: string;

}
export class KafkaError extends KafkaErrorAbstract {
    constructor(code: any, message: string);

    toJSON(): KafkaErrorAbstract;
    toString(): string;


}
export function byCode(code: any): null | KafkaError;
export function byName(name: string): null | KafkaError;

export class NoKafkaConnectionErrorAbstract {
    name: string;
    server: any;
    message: string;

}
export class NoKafkaConnectionError extends NoKafkaConnectionErrorAbstract {
    constructor(server: any, message: string);

    toJSON(): NoKafkaConnectionErrorAbstract;
    toString(): string;

}
