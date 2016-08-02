var assert = require('chai').assert;
var P = require('bluebird');
var Kafka = require('../lib');

var fakeConsumer = {};
fakeConsumer.commits = [];
fakeConsumer.commitOffset = function(payload) {
  fakeConsumer.commits.push({topic: payload.topic, partition: payload.partition, offset: payload.offset});
  var fakeThen = {};
  fakeThen.then = function() {
    var fakeCatch = {};
    fakeCatch.catch = function(){
      var fakeFinally = {};
      fakeFinally.finally = function(){};
      return fakeFinally;
    };
    return fakeCatch;
  };
  return fakeThen;
}
var fakeLog = {};
fakeLog.debug = function(){};
fakeLog.error = function(){};
fakeLog.info = function(){};

describe('PeriodicOffsetCommitter', function() {

  describe('#constructor()', function () {
    it('should create an object', function () {
      var periodicOffsetCommitter = new Kafka.PeriodicOffsetCommitter(fakeConsumer, fakeLog);
      assert.isNotNull(periodicOffsetCommitter, 'object should not be null');
    });
  });

  describe('#constructor()', function () {
    it('should throw an Error if required parameters are not passed', function () {
      try {
        new Kafka.PeriodicOffsetCommitter();
        assert.fail('no error', 'error', 'expected an error to be thrown');
      } catch (err) {
        // expected
        assert(true);
      }
    });
  });

  describe('#constructor()', function () {
    it('should allow an optional interval parameter to be passed', function () {
      var interval = 1000;
      var periodicOffsetCommitter = new Kafka.PeriodicOffsetCommitter(fakeConsumer, fakeLog, interval);
      assert.equal(periodicOffsetCommitter.interval, interval, 
        'expected interval of ' + interval + ' but actual value was ' + periodicOffsetCommitter.interval);
    });
  });

  describe('#setOffset()', function () {
    it('should set the offset', function (done) {
      var periodicOffsetCommitter = new Kafka.PeriodicOffsetCommitter(fakeConsumer, fakeLog, 100);
      periodicOffsetCommitter.setOffset('topicA', 0, 10);
      periodicOffsetCommitter.setOffset('topicA', 0, 11);
      periodicOffsetCommitter.setOffset('topicB', 7, 1);
      periodicOffsetCommitter.setOffset('topicB', 11, 1);
      periodicOffsetCommitter.setOffset('topicB', 7, 5);
      periodicOffsetCommitter.setOffset('topicA', 0, 9);
      setTimeout(function() {

        // wait to let the commitOffsetIfNeeded function get invoked
        assert(Object.keys(periodicOffsetCommitter.topics).length === 0
          && periodicOffsetCommitter.topics.constructor === Object,
          'expected topics to be empty object');

        // make sure the correct commits where made,
        // only the highest offset per topic/partition should be committed
        assert.deepEqual(fakeConsumer.commits[0], {topic: 'topicA', partition: 0, offset: 11});
        assert.deepEqual(fakeConsumer.commits[1], {topic: 'topicB', partition: 7, offset: 5});
        assert.deepEqual(fakeConsumer.commits[2], {topic: 'topicB', partition: 11, offset: 1});
        done();
      }, 500);
    });
  });

});