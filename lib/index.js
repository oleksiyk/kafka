"use strict";

module.exports = (function () {

    [
        'Producer',
        'Consumer'
    ].forEach(function (service) {
        exports[service] = require(__dirname + '/' + service.toLowerCase());
    });

    return exports;
})();
