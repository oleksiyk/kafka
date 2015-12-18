"use strict";

var logger = require('nice-simple-logger');
var _      = require('lodash');

module.exports = (function () {

    exports.Producer = require('./producer');
    exports.SimpleConsumer = require('./simple_consumer');

    // offset request time constants
    exports.EARLIEST_OFFSET = -2;
    exports.LATEST_OFFSET = -1;

    _.extend(exports, logger());

    return exports;
})();
