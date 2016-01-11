'use strict';

var eslint = require('gulp-eslint');
var gulp   = require('gulp');
var config = require('./config');

module.exports = function () {
    return gulp.src(config.eslint.paths)
        .pipe(eslint(config.eslint.options))
        .pipe(eslint.format(config.eslint.formatter))
        .pipe(eslint.failAfterError());
};
