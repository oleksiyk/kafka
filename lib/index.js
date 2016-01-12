'use strict';

module.exports = (function () {
    exports.Producer = require('./producer');
    exports.SimpleConsumer = require('./simple_consumer');
    exports.GroupConsumer = require('./group_consumer');

    // offset request time constants
    exports.EARLIEST_OFFSET = -2;
    exports.LATEST_OFFSET = -1;

    return exports;
})();
