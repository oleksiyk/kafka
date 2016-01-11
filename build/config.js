'use strict';

module.exports = {
    eslint: {
        formatter: 'stylish',
        paths: [
            'lib/**/*.js',
            'test/**/*.js'
        ],
        options: {}
    },

    mocha: {
        paths: [
            'test/**/*.js'
        ],
        options: {
            reporter: 'spec',
            require: ['./test/globals']
        }
    },

    istanbul: {
        paths: [
            'lib/**/*.js',
        ],
        outputPath: 'build/coverage',
        thresholds: {
            global: 70
        }
    }
};
