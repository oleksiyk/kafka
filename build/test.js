'use strict';

var gulp       = require('gulp');
var mocha      = require('gulp-mocha');
var istanbul = require('gulp-istanbul');

var config = require('./config');

module.exports = function () {
    return gulp.src(config.mocha.paths)
        .pipe(mocha(config.mocha.options))
        .pipe(istanbul.writeReports({
            dir: config.istanbul.outputPath,
        }))
        .pipe(istanbul.enforceThresholds({
            thresholds: config.istanbul.thresholds,
        }));
};
