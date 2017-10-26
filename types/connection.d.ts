/**
  * no-kafka will connect to the hosts specified in 
  * connectionString constructor option unless it is omitted.
  * In this case it will use KAFKA_URL environment
  * variable or fallback to default kafka://127.0.0.1:9092.
  * For better availability always specify several initial brokers:
  * 10.0.1.1:9092,10.0.1.2:9092,10.0.1.3:9092.
  * The / prefix is optional.
  * 
  * @export
  * @class Connection
  */
export class Connection {
    constructor(options: Options);
    equal(host: string, port: number): boolean;
    server(): string;
    connect(): Promise<any>;
    close(): void;

    send(correlationId: any, data: any, noresponse: any): Promise<any>;

}
export interface Options {
    port?: number;
    host?: string;
    connectionTimeout?: number;
    socketTimeout?: number;
    initialBufferSize?: number;
    ssl?: {
        cert: string;
        key: string;
    }
}
