'use strict';

var gulp       = require('gulp');
var istanbul = require('gulp-istanbul');

var config = require('./config');

module.exports = function () {
    return gulp.src(config.istanbul.paths)
        // Covering files
        .pipe(istanbul())
        // Force `require` to return covered files
        .pipe(istanbul.hookRequire());
};
