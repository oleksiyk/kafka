

declare module "kafka" {

    // offset request time constants
    
    export const LATEST_OFFSET = -1;
    export type OFFSET = -2 | -1;

    // compression codecs
    export const COMPRESSION_SNAPPY = 2;
    export const COMPRESSION_GZIP = 1;
    export const COMPRESSION_NONE = 0;
    export type COMPRESSION = 0 | 1 | 2;

    export interface Message {
        topic: string;
        partition?: string;
        message: {
            key?: string;
            value: string;
            attributes?: string[];
        }
    }
}
