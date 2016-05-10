'use strict';

// Default partitioner implementation that matches Java client:
// https://github.com/apache/kafka/blob/0.9.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java#L36
//

var murmur2 = require('murmur-hash-js').murmur2;

var SEED = 0x9747b28c;

function toPositive(n) {
    return n & 0x7fffffff;
}

function getRandomInt() {
    return Math.floor(Math.random() * 0xFFFFFFFF - 0x80000000);
}

function DefaultPartitioner(seed) {
    this.counter = typeof seed === 'number' ? seed | 0 : getRandomInt();
}

DefaultPartitioner.prototype.getAndIncrement = function () {
    var nextValue = this.counter;

    this.counter = this.counter + 1 | 0;

    return nextValue;
};

DefaultPartitioner.prototype.hashKey = function (key) {
    // Must convert to buffer before hashing due to issue with unicode
    // https://github.com/b3nj4m/murmurhash-js/pull/1
    var buf = Buffer.isBuffer(key) ? key : Buffer(key);

    return murmur2(buf, SEED);
};

DefaultPartitioner.prototype.getKey = function (message) {
    return message.key;
};

DefaultPartitioner.prototype.partition = function (topicName, partitions, message) {
    var key = this.getKey(message);

    // Round-robin partitioner
    if (key === undefined || key === null) {
        return toPositive(this.getAndIncrement()) % partitions.length;
    }

    // Hash partitioner
    return toPositive(this.hashKey(key)) % partitions.length;
};

module.exports = DefaultPartitioner;
