'use strict';

var protocol = require('bin-protocol');
var errors   = require('../errors');
var crc32    = require('buffer-crc32');
var _        = require('lodash');

/* jshint bitwise: false */

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

//////////////////////////
// PRIMITIVE DATA TYPES //
//////////////////////////

// variable length primitives, bytes
protocol.define('bytes', {
    read: function () {
        this.Int32BE('length');
        if (this.context.length <= 0) {
            return null;
        }
        this.raw('value', this.context.length);
        return this.context.value;
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.Int32BE(-1);
        } else {
            if (Buffer.isBuffer(value) || typeof value === 'string') {
                this
                    .Int32BE(value.length)
                    .raw(value);
            } else {
                throw new Error('Kafka bytes value should be a Buffer or String');
            }
        }
    }
});

// variable length primitives, string
protocol.define('string', {
    read: function () {
        this.Int16BE('length');
        if (this.context.length <= 0) {
            return null;
        }
        this.raw('value', this.context.length);
        return this.context.value.toString('utf8');
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.Int16BE(-1);
        } else {
            if (typeof value === 'string') {
                value = new Buffer(value, 'utf8');
                this
                    .Int16BE(value.length)
                    .raw(value);
            } else {
                throw new Error('Kafka string value should be a String');
            }
        }
    }
});

// array
protocol.define('array', {
    read: function (fn) {
        this.Int32BE('length');
        if (this.context.length <= 0) {
            return [];
        }
        this.loop('items', fn, this.context.length);
        return this.context.items;
    },
    write: function (value, fn) {
        if (value === null || value === undefined) {
            this.Int32BE(-1);
        } else {
            this
                .Int32BE(value.length)
                .loop(value, fn);
        }
    }
});

// return Error instance
protocol.define('ErrorCode', {
    read: function () {
        this.Int16BE('error');
        return errors.byCode(this.context.error);
    }
});

// 64 bit offset, convert from Long (https://github.com/dcodeIO/long.js) to double, 53 bit
protocol.define('KafkaOffset', {
    read: function () {
        this.Int64BE('offset');
        return this.context.offset.toNumber();
    },
    write: function (value) {
        this.Int64BE(value);
    }
});

protocol.define('RequestHeader', {
    write: function (header) {
        this
            .Int16BE(header.apiKey)
            .Int16BE(header.apiVersion)
            .Int32BE(header.correlationId)
            .string(header.clientId);
    }
});

////////////////////////////
// MESSAGE AND MESSAGESET //
////////////////////////////

protocol.define('MessageAttributes', {
    read: function () {
        this.Int8('_raw');
        this.context.codec = this.context._raw & 0x3;
    },
    write: function (codec) {
        this.Int8(codec & 0x3);
    }
});

protocol.define('Message', {
    read: function () {
        // var _o1 = this.offset;
        this
            .Int32BE('crc')
            .Int8('magicByte')
            .MessageAttributes('attributes')
            .bytes('key')
            .bytes('value');
        // crc32.signed(this.buffer.slice(_o1+4, this.offset));
    },
    write: function (value) { // {codec, magicByte, key, value}
        var _o1 = this.offset, _o2;
        this
            .skip(4)
            .Int8(value.magicByte || 0)
            .MessageAttributes(value.codec || 0)
            .bytes(value.key)
            .bytes(value.value);

        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(crc32.signed(this.buffer.slice(_o1 + 4, _o2)));
        this.offset = _o2;
    }
});

protocol.define('MessageSetItem', {
    read: function (size, end) {
        if (size < 8 + 4) {
            this.skip(size);
            return end();
        }
        this
            .KafkaOffset('offset')
            .Int32BE('messageSize');

        if (size < 8 + 4 + this.context.messageSize) {
            this.skip(size - 8 - 4);
            this.context._partial = true;
            return end();
        }

        this.Message('message');
    },
    write: function (value) { // {offset, message}
        var _o1, _o2;
        this.Int64BE(value.offset);
        _o1 = this.offset;
        this
            .skip(4)
            .Message(value.message);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

protocol.define('MessageSet', {
    read: function (size) {
        var _o1 = this.offset;
        if (!size) {
            return [];
        }
        this.loop('items', function (end) {
            this.MessageSetItem(null, size - this.offset + _o1, end);
        });

        return _.dropRightWhile(this.context.items, { _partial: true }); // drop partailly read messages
    },
    write: function (items) {
        this.loop(items, this.MessageSetItem);
    }
});
