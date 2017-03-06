'use strict';

var Client = require('./client');
var Promise = require('./bluebird-configured');

function GroupAdmin(options) {
    this.client = new Client(options);
}

module.exports = GroupAdmin;


/**
 * Initialize GroupAdmin
 *
 * @return {Promise}
 */
GroupAdmin.prototype.init = function () {
    return this.client.init();
};

/**
 * List all consumer groups
 *
 * @return {Promise}
 */
GroupAdmin.prototype.listGroups = function () {
    return this.client.listGroupsRequest();
};

/**
 * Describe consumer group
 *
 * @param  {String} groupId
 * @return {Promise}
 */
GroupAdmin.prototype.describeGroup = function (groupId) {
    return this.client.describeGroupRequest(groupId);
};

/**
 * Provides output similar to
 * kafka.admin.ConsumerGroupCommand --describe
 *
 * @param  {String} groupId
 * @param  {Array} offsetFetchRequestArgs
 * @return {Promise}
 */
GroupAdmin.prototype.fetchConsumerLag = function (groupId, offsetFetchRequestArgs) {
    return Promise.all([
        this.client.offsetFetchRequestV1(groupId, offsetFetchRequestArgs),
        this._fetchHighWaterMark(offsetFetchRequestArgs)
    ]).spread(function (offsets, highWaterMark) {
        var i, j, res;
        var responses = [];

        // Match the returned data into a coherent response
        for (i = 0; i < offsets.length; i++) {
            res = {
                topic: offsets[i].topic,
                partition: offsets[i].partition,
                offset: offsets[i].offset,
                highwaterMark: null,
                consumerLag: null
            };

            for (j = 0; j < highWaterMark.length; j++) {
                if (
                    offsets[i].topic === highWaterMark[j].topic
                    && offsets[i].partition === highWaterMark[j].partition
                ) {
                    res.highwaterMark = highWaterMark[j].highWaterMark;
                    if (res.offset > 0 && res.highwaterMark > 0) {
                        res.consumerLag = res.highwaterMark - res.offset;
                    }
                }
            }

            responses.push(res);
        }

        return responses;
    });
};

GroupAdmin.prototype._fetchHighWaterMark = function (offsetFetchRequestArgs) {
    var topicName, topicPartitions, i, j;
    var offsetRequestsPromises = [];

    for (i = 0; i < offsetFetchRequestArgs.length; i++) {
        topicName = offsetFetchRequestArgs[i].topicName;
        topicPartitions = offsetFetchRequestArgs[i].partitions;

        for (j = 0; j < topicPartitions.length; j++) {
            offsetRequestsPromises.push(
                this._fetchHighWaterMarkOffset(topicName, topicPartitions[j])
            );
        }
    }

    return Promise.all(offsetRequestsPromises);
};

GroupAdmin.prototype._fetchHighWaterMarkOffset = function (topicName, partition) {
    var self = this;
    return self.client.findLeader(topicName, partition).then(function (leader) {
        return self.client.getPartitionOffset(leader, topicName, partition);
    }).then(function (offset) {
        return {
            topic: topicName,
            partition: partition,
            highWaterMark: offset
        };
    });
};

/**
 * Close all connections
 *
 * @return {Promise}
 */
GroupAdmin.prototype.end = function () {
    var self = this;

    return self.client.end();
};
