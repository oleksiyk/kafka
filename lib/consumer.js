"use strict";

// var Promise    = require('bluebird');
var _ = require('lodash');
var Client = require('./client');

function Consumer (options){
    this.options = _.partialRight(_.merge, _.defaults)(options || {}, {
        timeout: 100,
        minBytes: 1,
        maxBytes: 1024 * 1024
    });

    this.client = new Client(this.options);

    this.subscriptions = {};
}

module.exports = Consumer;

Consumer.prototype.init = function() {
    return this.client.init();
};

Consumer.prototype.subscribe = function(topic, partition, offset) {
    var self = this;

    if(!self.subscriptions[topic]){
        self.subscriptions[topic] = {};
    }

    if(!self.subscriptions[topic][partition]){
        self.subscriptions[topic][partition] = offset || 0;
    }

    // return self.client.fetchRequest(self.subscriptions);
};

Consumer.prototype.unsubscribe = function(topic, partition) {
    var self = this;

    if(partition === undefined){
        if(self.subscriptions[topic]){
            delete self.subscriptions[topic];
        }
    } else if(self.subscriptions[topic] && self.subscriptions[topic][partition]){
        delete self.subscriptions[topic][partition];
        if(_.isEmpty(self.subscriptions[topic])){
            delete self.subscriptions[topic];
        }
    }
};
