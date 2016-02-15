'use strict';

var Promise = require('bluebird');
var snappy  = require('snappy');
var zlib    = require('zlib');
var _ = require('lodash');

var SNAPPY_MAGIC_HEADER = new Buffer([-126, 83, 78, 65, 80, 80, 89, 0]); // '\x82SNAPPY\00'
/*var SNAPPY_BLOCK_SIZE = 32 * 1024;
var SNAPPY_DEFAULT_VERSION = 1;
var SNAPPY_MINIMUM_COMPATIBLE_VERSION = 1;*/

var Snappy, Gzip;

Snappy = {
    _chunks: function (buffer) {
        var offset = 16, size, chunks = [];
        if (buffer.toString('hex', 0, 8) === SNAPPY_MAGIC_HEADER.toString('hex')) {
            // var defaultVersion = buffer.readUInt32BE(offset); offset += 4;
            // var minimumVersion = buffer.readUInt32BE(offset); offset += 4;
            while (offset < buffer.length) {
                size = buffer.readUInt32BE(offset); offset += 4;
                chunks.push(buffer.slice(offset, offset + size));
                offset += size;
            }
        } else {
            /* istanbul ignore next */
            chunks = [buffer];
        }
        return chunks;
    },

    _uncompressAsync: _.ary(Promise.promisify(snappy.uncompress), 1),

    decompress: function (buffer) {
        return Buffer.concat(Snappy._chunks(buffer).map(_.ary(snappy.uncompressSync, 1)));
    },

    decompressAsync: function (buffer) {
        return Promise.map(Snappy._chunks(buffer), Snappy._uncompressAsync).then(Buffer.concat);
    },

    compress: snappy.compressSync,

    compressAsync: _.ary(Promise.promisify(snappy.compress), 1)
};

Gzip = {
    decompress: zlib.gunzipSync,

    decompressAsync: _.ary(Promise.promisify(zlib.gunzip), 1),

    compress: zlib.gzipSync,

    compressAsync: _.ary(Promise.promisify(zlib.gzip), 1),
};

module.exports = {
    decompress: function (buffer, codec) {
        if (codec === 2) {
            return Snappy.decompress(buffer);
        } else if (codec === 1 && typeof zlib.gunzipSync === 'function') {
            return Gzip.decompress(buffer);
        }
        /* istanbul ignore next */
        throw new Error('Unsupported compression codec ' + codec);
    },
    decompressAsync: function (buffer, codec) {
        if (codec === 2) {
            return Snappy.decompressAsync(buffer);
        } else if (codec === 1) {
            return Gzip.decompressAsync(buffer);
        }
        /* istanbul ignore next */
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    },
    compress: function (buffer, codec) {
        if (codec === 2) {
            return Snappy.compress(buffer);
        } else if (codec === 1 && typeof zlib.gzipSync === 'function') {
            return Gzip.compress(buffer);
        }
        throw new Error('Unsupported compression codec ' + codec);
    },
    compressAsync: function (buffer, codec) {
        if (codec === 2) {
            return Snappy.compressAsync(buffer);
        } else if (codec === 1) {
            return Gzip.compressAsync(buffer);
        }
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    }
};
