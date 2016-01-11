'use strict';

var gulp     = require('gulp');
var eslint   = require('./build/eslint');
var test     = require('./build/test');
var coverage = require('./build/coverage');

gulp.task('lint', eslint);
gulp.task('coverage', coverage);
gulp.task('test', ['lint', 'coverage'], test);

gulp.task('default', ['test']);
