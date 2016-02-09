'use strict';

var snappy   = require('snappy');
var zlib     = require('zlib');

var SNAPPY_MAGIC_HEADER = new Buffer([-126, 83, 78, 65, 80, 80, 89, 0]); // '\x82SNAPPY\00'
var SNAPPY_BLOCK_SIZE = 32 * 1024;
var SNAPPY_DEFAULT_VERSION = 1;
var SNAPPY_MINIMUM_COMPATIBLE_VERSION = 1;

var Snappy, Gzip;

Snappy = {
    decompress: function (buffer) {
        var offset = 16, size, chunks = [];
        if (buffer.toString('hex', 0, 8) === SNAPPY_MAGIC_HEADER.toString('hex')) {
            // var defaultVersion = buffer.readUInt32BE(offset); offset += 4;
            // var minimumVersion = buffer.readUInt32BE(offset); offset += 4;
            while (offset < buffer.length) {
                size = buffer.readUInt32BE(offset); offset += 4;
                chunks.push(buffer.slice(offset, offset + size)); offset += size;
            }
            return snappy.uncompressSync(Buffer.concat(chunks));
        }
        return snappy.uncompressSync(buffer);
    },

    compress: function (buffer) {
        var offset = 0, chunks = [];

        if (buffer.length > SNAPPY_BLOCK_SIZE) {
            chunks.push(SNAPPY_MAGIC_HEADER);
            chunks.push(new Buffer(8));
            chunks[chunks.length - 1].writeUInt32BE(SNAPPY_DEFAULT_VERSION);
            chunks[chunks.length - 1].writeUInt32BE(SNAPPY_MINIMUM_COMPATIBLE_VERSION);

            while (offset < buffer.length) {
                chunks.push(new Buffer(4));
                chunks.push(snappy.compressSync(buffer.slice(offset, offset + SNAPPY_BLOCK_SIZE))); offset += SNAPPY_BLOCK_SIZE;
                chunks[chunks.length - 2].writeUInt32BE(chunks[chunks.length - 1].length);
            }

            return Buffer.concat(chunks);
        }
        return snappy.compressSync(buffer);
    }
};

Gzip = {
    decompress: function (buffer) {
        return zlib.gunzipSync(buffer);
    },
    compress: function (buffer) {
        return zlib.gzipSync(buffer);
    }
};

module.exports = {
    decompress: function (buffer, codec) {
        if (codec === 2) {
            return Snappy.decompress(buffer);
        } else if (codec === 1 && typeof zlib.gunzipSync === 'function') {
            return Gzip.decompress(buffer);
        }
        throw new Error('Unsupported compression codec ' + codec);
    }
};
