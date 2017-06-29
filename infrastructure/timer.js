'use strict';

function getMs(timeArray) {
    return timeArray[0] * 1000 // to get miliseconds from seconds
    + timeArray[1] / 1000000; // to get from nanoseconds to miliseconds
}

module.exports = class Timer {
    constructor() {
        this.start = process.hrtime();
    }

    getTime() {
        return getMs(process.hrtime(this.start));
    }

    static start() {
        return new Timer();
    }
};
