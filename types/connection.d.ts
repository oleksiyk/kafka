

declare module "connection" {
    export class Connection {
        constructor(options: Options);
equal(host: string, port: number): boolean;
server(): string;
connect(): Promise<any>;
close(): void;

send(correlationId: any, data: any, noresponse:any): Promise<any>;

    }
    export interface Options {
        port: number;
        host: string;
        connectionTimeout: number;
        initialBufferSize: number;
        ssl: {

        }
    }
}