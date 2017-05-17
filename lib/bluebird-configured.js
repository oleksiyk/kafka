'use strict';

var Promise = require('bluebird/js/release/promise')();

Promise.config({
    cancellation: true,
    /*warnings: true,
    monitoring: true,
    longStackTraces: true*/
});

module.exports = Promise;
