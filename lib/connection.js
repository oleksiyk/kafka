"use strict";

var net          = require('net');
var Promise      = require('bluebird');

function Connection(options) {
    // options
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 9092;
    this.auto_connect = options.auto_connect || true;

    // internal state
    this.connected = false;
    this.readBuf = new Buffer(256 * 1024);
    this.readBufPos = 0;

    this.queue = [];
}

module.exports = Connection;

Connection.prototype.equal = function(host, port) {
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
            setTimeout(function() {
                reject(new Error('Connection timeout to ' + self.host + ':' + self.port));
            }, timeout || 3000);
        }),
        new Promise(function (resolve) {
            if(self.socket){
                self.socket.destroy();
            }

            self.socket = new net.Socket();
            self.socket.on('end', function () {
                self._disconnect(new Error('Kafka server has closed connection'));
            });
            self.socket.on('error', self._disconnect.bind(self));
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

Connection.prototype._growReadBuffer = function (newLength) {
    var _b = new Buffer(newLength);
    this.readBuf.copy(_b, 0, 0, this.readBufPos);
    this.readBuf = _b;
};

Connection.prototype.close = function () {
    this.auto_connect = false;
    this._disconnect({ _kafka_connection_closed: true });
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

    function _send (){
        return new Promise(function (resolve, reject) {
            self.queue.push({
                resolve: resolve,
                reject: reject
            });

            self.socket.write(buffer);

            if(noresponse === true){
                self.queue.shift().resolve();
            }
        });
    }

    if(!self.connected){
        if(self.auto_connect){
            return self.connect().then(function () {
                return _send();
            });
        } else {
            return Promise.reject(new Error('Not connected'));
        }
    }

    return _send();
};

Connection.prototype._receive = function (data) {
    var length;

    if (!this.connected) {
        return;
    }

    if (this.readBufPos) {
        if (this.readBuf.length < data.length + this.readBufPos) {
            this._growReadBuffer(data.length + this.readBufPos);
        }
        data.copy(this.readBuf, this.readBufPos);
        this.readBufPos += data.length;
        data = this.readBuf.slice(0, this.readBufPos);
    }

    length = data.length < 4 ? 0 : data.readInt32BE(0);

    if (data.length < 4 + length) {
        if (this.readBufPos === 0) {
            if (this.readBuf.length < 4 + length) {
                this._growReadBuffer(4 + length);
            }
            data.copy(this.readBuf);
            this.readBufPos += data.length;
        }
        return;
    }

    this.readBufPos = 0;

    this.queue.shift().resolve(data.slice(4, length + 4));

    if (data.length > 4 + length) {
        this._receive(data.slice(length + 4));
    }
};

