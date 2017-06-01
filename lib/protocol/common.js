'use strict';

var Protocol = require('./index');
var errors   = require('../errors');
var crc32    = require('buffer-crc32');
var _        = require('lodash');

/* jshint bitwise: false */

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

//////////////////////////
// PRIMITIVE DATA TYPES //
//////////////////////////

// variable length primitives, bytes
Protocol.define('bytes', {
    read: function () {
        this.Int32BE('length');
        if (this.context.length < 0) {
            return null;
        }
        this.raw('value', this.context.length);
        return this.context.value;
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.Int32BE(-1);
        } else {
            if (!Buffer.isBuffer(value)) {
                value = new Buffer(_(value).toString(), 'utf8');
            }
            this
                .Int32BE(value.length)
                .raw(value);
        }
    }
});

// variable length primitives, string
Protocol.define('string', {
    read: function () {
        this.Int16BE('length');
        if (this.context.length < 0) {
            return null;
        }
        this.raw('value', this.context.length);
        return this.context.value.toString('utf8');
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.Int16BE(-1);
        } else {
            value = new Buffer(_(value).toString(), 'utf8');
            this
                .Int16BE(value.length)
                .raw(value);
        }
    }
});

// array
Protocol.define('array', {
    read: function (fn) {
        this.Int32BE('length');
        if (this.context.length < 0) {
            return null;
        } else if (this.context.length === 0) {
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
Protocol.define('ErrorCode', {
    read: function () {
        this.Int16BE('error');
        return errors.byCode(this.context.error);
    }
});

// 64 bit offset, convert from Long (https://github.com/dcodeIO/long.js) to double, 53 bit
Protocol.define('KafkaOffset', {
    read: function () {
        this.Int64BE('offset');
        return this.context.offset.toNumber();
    },
    write: function (value) {
        this.Int64BE(value);
    }
});

Protocol.define('RequestHeader', {
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

Protocol.define('MessageAttributes', {
    read: function () {
        this.Int8('_raw');
        this.context.codec = this.context._raw & 0x3;
    },
    write: function (attributes) {
        this.Int8((attributes.codec || 0) & 0x3);
    }
});

Protocol.define('Message', {
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
    write: function (value) { // {attributes, magicByte, key, value}
        var _o1 = this.offset, _o2;
        this
            .skip(4)
            .Int8(value.magicByte || 0)
            .MessageAttributes(value.attributes || {})
            .bytes(value.key)
            .bytes(value.value);

        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(crc32.signed(this.buffer.slice(_o1 + 4, _o2)));
        this.offset = _o2;
    }
});

Protocol.define('MessageSetItem', {
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

        return undefined;
    },
    write: function (value) { // {offset, message}
        var _o1, _o2;
        this.KafkaOffset(value.offset);
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

Protocol.define('MessageSet', {
    read: function (size) {
        var _o1 = this.offset;
        if (!size) {
            return [];
        }
        this.loop('items', function (end) {
            this.MessageSetItem(null, size - this.offset + _o1, end);
        });

        return this.context.items;
    },
    write: function (items) {
        this.loop(items, this.MessageSetItem);
    }
});
