'use strict';

var Client = require('./client');

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
 * Close all connections
 *
 * @return {Promise}
 */
GroupAdmin.prototype.end = function () {
    var self = this;

    return self.client.end();
};
