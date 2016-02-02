'use strict';

var net                    = require('net');
var Promise                = require('bluebird');
var NoKafkaConnectionError = require('./errors').NoKafkaConnectionError;

function Connection(options) {
    // options
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 9092;

    // internal state
    this.connected = false;
    this.buffer = new Buffer(256 * 1024);
    this.offset = 0;

    this.queue = [];
}

module.exports = Connection;

Connection.prototype.equal = function (host, port) {
    return this.host === host && this.port === port;
};

Connection.prototype.connect = function (timeout) {
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
                reject(new NoKafkaConnectionError(self, 'Connection timeout to ' + self.host + ':' + self.port));
            }, timeout || 3000);
        }),
        new Promise(function (resolve, reject) {
            if (self.socket) {
                self.socket.destroy();
            }

            self.socket = new net.Socket();
            self.socket.on('end', function () {
                self._disconnect(new NoKafkaConnectionError(self, 'Kafka server [' + self.host + ':' + self.port + '] has closed connection'));
            });
            self.socket.on('error', function (err) {
                reject(err);
                self._disconnect(err);
            });
            self.socket.on('data', self._receive.bind(self));

            self.socket.connect(self.port, self.host, function () {
                self.connected = true;
                resolve();
            });
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

    this.queue.forEach(function (t) {
        t.reject(err);
    });

    this.queue = [];
};

Connection.prototype._growBuffer = function (newLength) {
    var _b = new Buffer(newLength);
    this.buffer.copy(_b, 0, 0, this.offset);
    this.buffer = _b;
};

Connection.prototype.close = function () {
    var err = new NoKafkaConnectionError(this, 'Connection closed');
    err._kafka_connection_closed = true;
    this._disconnect(err);
};

/**
 * Send a request to Kafka
 *
 * @param  {Buffer} data request message
 * @param  {Boolean} noresponse if the server wont send any response to this request
 * @return {Promise}      Promise resolved with a Kafka response message
 */
Connection.prototype.send = function (data, noresponse) {
    var self = this, buffer = new Buffer(4 + data.length);

    buffer.writeInt32BE(data.length, 0);
    data.copy(buffer, 4);

    function _send() {
        return new Promise(function (resolve, reject) {
            self.queue.push({
                resolve: resolve,
                reject: reject
            });

            self.socket.write(buffer);

            if (noresponse === true) {
                self.queue.shift().resolve();
            }
        });
    }

    if (!self.connected) {
        return self.connect().then(function () {
            return _send();
        });
    }

    return _send();
};

Connection.prototype._receive = function (data) {
    var length;

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

    this.queue.shift().resolve(data.slice(4, length + 4));

    if (data.length > 4 + length) {
        this._receive(data.slice(length + 4));
    }
};
