'use strict';

/* global describe, it */

var util = require('util');
var _    = require('lodash');

var DefaultPartitioner = require('../lib/default_partitioner');

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
        var partitioner = new DefaultPartitioner(0);

        partitioner.partition('topic', new Array(10), {}).should.equal(0);
    });

    it('should increment after each call to round robin partitioner', function () {
        var partitioner = new DefaultPartitioner(0);

        partitioner.partition('topic', new Array(10), {}).should.equal(0);
        partitioner.partition('topic', new Array(10), {}).should.equal(1);
        partitioner.partition('topic', new Array(10), {}).should.equal(2);
    });

    it('should choose random seed if one is not provided', function () {
        var partitioner = new DefaultPartitioner();
        var seed = partitioner.partition('topic', new Array(10), {});

        partitioner.partition('topic', new Array(10), {}).should.equal((seed + 1) % 10);
    });

    it('should partition correctly with murmur2', function () {
        var partitioner = new DefaultPartitioner();

        _.each(expectedPartitions, function (partition, key) {
            partitioner.partition('topic', new Array(numPartitions), {
                key : key
            }).should.equal(partition);

            partitioner.partition('topic', new Array(numPartitions), {
                key : Buffer(key)
            }).should.equal(partition);
        });
    });

    it('should be able to inherit from DefaultPartitioner', function () {
        var partitioner;

        function MyPartitioner() {
            DefaultPartitioner.apply(this, arguments);
        }

        util.inherits(MyPartitioner, DefaultPartitioner);

        MyPartitioner.prototype.getKey = function getKey(message) {
            return message.key.split('-')[0];
        };

        partitioner = new MyPartitioner();

        partitioner.partition('topic', new Array(12), {
            key : 'msg 1-Something else'
        }).should.equal(6);
    });
});
