'use strict';

let _ = require('lodash');

const SAMPLE_RATE = 1;

module.exports = class {
    constructor(client, tags) {
        this.client = client;
        this.defaultTags = tags;
    }

    reportGauge(metricName, value, tags) {
        this.client.gauge(metricName, value, SAMPLE_RATE,
        this._getAllCompatibleTags(tags));
    }

    reportTiming(metricName, value, tags) {
        this.client.timing(metricName, value, SAMPLE_RATE,
        this._getAllCompatibleTags(tags));
    }

    _getAllCompatibleTags(tags) {
        let allTags = _.assign({}, this.defaultTags, tags);
        return _.reduce(allTags, function (memo, v, k) {
            memo.push(`${k}:${v}`); return memo;
        }, []);
    }
};
