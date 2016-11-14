'use strict';

/* global describe, it */

var util  = require('util');
var _     = require('lodash');
var Kafka = require('../lib/index');

// Ran against Java DefaultPartitioner with 12 partitions
// https://gist.github.com/kjvalencik/74ff9c7451878d150819
// https://github.com/apache/kafka/blob/0.9.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java
var numPartitions = 120;
var expectedPartitions = {
    '\u263a tlw61or'         : 101,
    '\u263a ww50w20ggb9'     : 26,
    '\u263a tf706z9i2v86w29' : 40,
    '\u263a pgb9'            : 1,
    '\u263a b9'              : 8
};

describe('Default Partitioner', function () {
    it('should be able to seed round robin partitioner', function () {
        var partitioner = new Kafka.DefaultPartitioner(0);

        partitioner.partition('topic', new Array(10), {}).should.equal(0);
    });

    it('should increment after each call to round robin partitioner', function () {
        var partitioner = new Kafka.DefaultPartitioner(0);

        partitioner.partition('topic', new Array(10), {}).should.equal(0);
        partitioner.partition('topic', new Array(10), {}).should.equal(1);
        partitioner.partition('topic', new Array(10), {}).should.equal(2);
    });

    it('should choose random seed if one is not provided', function () {
        var partitioner = new Kafka.DefaultPartitioner();
        var seed = partitioner.partition('topic', new Array(10), {});

        partitioner.partition('topic', new Array(10), {}).should.equal((seed + 1) % 10);
    });

    it('should partition correctly with murmur2', function () {
        var partitioner = new Kafka.DefaultPartitioner();

        _.each(expectedPartitions, function (partition, key) {
            partitioner.partition('topic', new Array(numPartitions), {
                key : key
            }).should.equal(partition);

            partitioner.partition('topic', new Array(numPartitions), {
                key : Buffer(key)
            }).should.equal(partition);
        });
    });

    it('should be able to inherit from Kafka.DefaultPartitioner', function () {
        var partitioner;

        function MyPartitioner() {
            Kafka.DefaultPartitioner.apply(this, arguments);
        }

        util.inherits(MyPartitioner, Kafka.DefaultPartitioner);

        MyPartitioner.prototype.getKey = function (message) {
            return message.key.split('-')[0];
        };

        partitioner = new MyPartitioner();

        partitioner.partition('topic', new Array(12), {
            key : 'msg 1-Something else'
        }).should.equal(6);
    });
});

describe('Hash CRC32 Partitioner', function () {
    it('should be able to handle missing key', function () {
        var partitioner = new Kafka.HashCRC32Partitioner();

        partitioner.partition('topic', new Array(10), {}).should.equal(0);
    });

    it('should partition correctly with crc32', function () {
        var partitioner = new Kafka.HashCRC32Partitioner();

        partitioner.partition('topic', new Array(10), {
            key: 'test-key'
        }).should.equal(1);

        partitioner.partition('topic', new Array(7), {
            key: 'test-key'
        }).should.equal(5);
    });
});
