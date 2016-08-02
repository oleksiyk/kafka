"use strict";
var P = require('bluebird');

function PeriodicOffsetCommitter(consumer, bunyanStyleLogger, interval, logDuration) {
  if (!consumer) {
    throw new Error("consumer is required");
  }
  this.interval = interval ? interval : 5000;
  this.logDuration = logDuration ? logDuration : defaultLogDuration;
  this.consumer = consumer;
  this.log = bunyanStyleLogger;
  this.topics = {};

  if (this.log) this.log.debug("calling commitOffsetIfNeeded every " + this.interval + "ms");
  setTimeout(commitOffsetIfNeeded.bind(this), this.interval);
}

PeriodicOffsetCommitter.prototype.setOffset = function(topic, partition, offset) {
  if (this.log) this.log.debug({topic: topic, partition: partition, offset: offset}, "setOffset");
  if (!this.topics[topic]) {
    this.topics[topic] = {partitions: {}};
  }
  var partitions = this.topics[topic].partitions;
  if (!partitions[partition]) {
    partitions[partition] = -1;
  }
  if (offset > partitions[partition]) {
    partitions[partition] = offset;
  }
};

function commitOffsetIfNeeded() {
  if (this.log) this.log.debug("commitOffsetIfNeeded");
  var self = this;
  var payloads = [];
  for (var topicKey in this.topics) {
    var partitions = this.topics[topicKey].partitions;
    for (var partitionKey in partitions) {
      var offset = partitions[partitionKey];
      payloads.push({topic: topicKey, partition: parseInt(partitionKey), offset: offset});
    }
  }
  return P.each(payloads, function(payload) {
    return commitOffset(payload, self.consumer, self.log, self.logDuration);
  })
  .then(function() {
    self.topics = {};
    setTimeout(commitOffsetIfNeeded.bind(self), self.interval);
  });
}

function commitOffset(payload, consumer, log, logDuration) {
  var start = new Date();
  return consumer.commitOffset(payload)
    .then(function() {
      if (log) log.info(payload, "consumer.commitOffset");
    })
    .catch(function(err) {
      if (log) log.error(err);
    })
    .finally(function() {
      if (log) logDuration(log, start, "consumer.commitOffset");
    });
}

/**
 * Default implementation of logging a duration.  Overrideable via the constructor
 * because the specific format of a performance log likely differs per organization.
 */
function defaultLogDuration(log, start, label) {
  log.info({duration: new Date().getTime() - start}, label);
}

module.exports = PeriodicOffsetCommitter;
