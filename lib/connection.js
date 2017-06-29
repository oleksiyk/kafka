'use strict';

var net                    = require('net');
var Promise                = require('./bluebird-configured');
var NoKafkaConnectionError = require('./errors').NoKafkaConnectionError;
var tls                    = require('tls');
var _                      = require('lodash');
var StatsD      = require('node-statsd');
var MetricReporter = require('../infrastructure/metric_reporter');
var Timer = require('../infrastructure/timer');

function Connection(options) {
    this.options = _.defaults(options || {}, {
        port: 9092,
        host: '127.0.0.1',
        connectionTimeout: 3000,
        initialBufferSize: 256 * 1024,
        ssl: {}
    });

    // internal state
    this.connected = false;
    this.closed = false; // raised if close() was called
    this.buffer = new Buffer(this.options.initialBufferSize);
    this.offset = 0;

    this.queue = {};

    let statsdClient = new StatsD();
    this.metricReporter = new MetricReporter(statsdClient, null);
}

module.exports = Connection;

Connection.prototype.equal = function (host, port) {
    return this.options.host === host && this.options.port === port;
};

Connection.prototype.server = function () {
    return this.options.host + ':' + this.options.port;
};

Connection.prototype.connect = function () {
    var self = this;

    if (self.connected) {
        return Promise.resolve();
    }

    if (self.connecting) {
        return self.connecting;
    }

    self.connecting = Promise.race([
        new Promise(function (resolve, reject) {
            setTimeout(function () {
                reject(new NoKafkaConnectionError(self.server(), 'Connection timeout'));
            }, self.options.connectionTimeout);
        }),
        new Promise(function (resolve, reject) {
            var onConnect = function () {
                self.connected = true;
                resolve();
            };

            if (self.socket) {
                self.socket.destroy();
            }

            // If we have aborted/stopped during startup, don't progress.
            if (self.closed) {
                reject(new NoKafkaConnectionError(self.server(), 'Connection was aborted before connection was established.'));
                return;
            }

            if (self.options.ssl && self.options.ssl.cert && self.options.ssl.key) {
                self.socket = tls.connect(self.options.port, self.options.host, self.options.ssl, onConnect);
            } else {
                self.socket = net.connect(self.options.port, self.options.host, onConnect);
            }

            self.socket.on('end', function () {
                self._disconnect(new NoKafkaConnectionError(self.server(), 'Kafka server has closed connection'));
            });
            self.socket.on('error', function (err) {
                var _err = new NoKafkaConnectionError(self.server(), err.toString());
                reject(_err);
                self._disconnect(_err);
            });
            self.socket.on('data', self._receive.bind(self));
        })
    ])
    .finally(function () {
        self.connecting = false;
    });

    return self.connecting;
};

// Private disconnect method, this is what the 'end' and 'error'
// events call directly to make sure internal state is maintained
Connection.prototype._disconnect = function (err) {
    if (!this.connected) {
        return;
    }

    this.socket.end();
    this.connected = false;

    _.each(this.queue, function (t) {
        t.reject(err);
    });

    this.queue = {};
};

Connection.prototype._growBuffer = function (newLength) {
    var _b = new Buffer(newLength);
    this.buffer.copy(_b, 0, 0, this.offset);
    this.buffer = _b;
};

Connection.prototype.close = function () {
    var err = new NoKafkaConnectionError(this, 'Connection closed');
    err._kafka_connection_closed = true;
    this.closed = true;
    this._disconnect(err);
};

/**
 * Send a request to Kafka
 *
 * @param  {Buffer} data request message
 * @param  {Boolean} noresponse if the server wont send any response to this request
 * @return {Promise}      Promise resolved with a Kafka response message
 */

Connection.prototype.send = function (correlationId, data, noresponse) {
    let sendTime = Timer.start();
    var self = this, buffer = new Buffer(4 + data.length);

    buffer.writeInt32BE(data.length, 0);
    data.copy(buffer, 4);

    function _send() {
        return new Promise(function (resolve, reject) {
            var _sendTime = Timer.start();
            self.queue[correlationId] = {
                resolve: resolve,
                reject: reject,
            };
            self.socket.write(buffer, () => {
                self.metricReporter.reportTiming('edgebroker.connection._send', _sendTime.getTime());
                self.queue[correlationId].timer = Timer.start();
            });
            if (noresponse === true) {
                self.queue[correlationId].resolve();
                delete self.queue[correlationId];
            }
        });
    }

    if (!self.connected) {
        return self.connect().then(function () {
            return _send();
        });
    }
    self.metricReporter.reportTiming('edgebroker.connection.send', sendTime.getTime());
    return _send();
};

Connection.prototype._receive = function (data) {
    var length, correlationId;

    if (!this.connected) {
        return;
    }

    if (this.offset) {
        if (this.buffer.length < data.length + this.offset) {
            this._growBuffer(data.length + this.offset);
        }
        data.copy(this.buffer, this.offset);
        this.offset += data.length;
        data = this.buffer.slice(0, this.offset);
    }

    length = data.length < 4 ? 0 : data.readInt32BE(0);

    if (data.length < 4 + length) {
        if (this.offset === 0) {
            if (this.buffer.length < 4 + length) {
                this._growBuffer(4 + length);
            }
            data.copy(this.buffer);
            this.offset += data.length;
        }
        return;
    }

    this.offset = 0;

    correlationId = data.readInt32BE(4);

    /*if (!this.queue.hasOwnProperty(correlationId)) {
        console.error('Wrong correlationId received:', correlationId);
    }*/
    this.metricReporter.reportTiming('edgebroker.connection.receive_ack', this.queue[correlationId].timer.getTime());

    this.queue[correlationId].resolve(new Buffer(data.slice(4, length + 4)));
    delete this.queue[correlationId];

    if (data.length > 4 + length) {
        this._receive(data.slice(length + 4));
    }
};
