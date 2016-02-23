'use strict';

var Promise = require('bluebird/js/release/promise')();

Promise.config({
    cancellation: true
});

module.exports = Promise;
