[![Build Status][badge-travis]][travis]
[![Test Coverage][badge-coverage]][coverage]
[![david Dependencies][badge-david-deps]][david-deps]
[![david Dev Dependencies][badge-david-dev-deps]][david-dev-deps]
[![license][badge-license]][license]

# no-kafka

__no-kafka__ is [Apache Kafka](https://kafka.apache.org) 0.9 client for Node.js with [new unified consumer API](#groupconsumer-new-unified-consumer-api) support.

Supports sync and async Gzip and Snappy compression, producer batching and controllable retries, offers few predefined group assignment strategies and producer partitioner option.

All methods will return a [promise](https://github.com/petkaantonov/bluebird)

__Please check a [CHANGELOG](CHANGELOG.md) for backward incompatible changes in version 3.x__

* [Using](#using)
* [Producer](#producer)
  * [Keyed Messages](#keyed-messages)
  * [Batching (grouping) produce requests](#batching-grouping-produce-requests)
  * [Custom Partitioner](#custom-partitioner)
  * [Producer options](#producer-options)
* [Simple Consumer](#simpleconsumer)
  * [Simple Consumer options](#simpleconsumer-options)
* [Group Consumer](#groupconsumer-new-unified-consumer-api)
  * [Assignment strategies](#assignment-strategies)
  * [Group Consumer options](#groupconsumer-options)
* [Group Admin](#groupadmin-consumer-groups-api)
* [Compression](#compression)
* [Connection](#connection)
  * [SSL](#ssl)
  * [Remapping Broker Addresses](#remapping-broker-addresses)
* [Logging](#logging)
* [Topic Creation](#topic-creation)
* [License](#license-mit)

## Using

* [download and install Kafka](https://kafka.apache.org/documentation.html#quickstart)
* create your test topic:

```shell
kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic kafka-test-topic --partitions 3 --replication-factor 1
```

* install __no-kafka__

```shell
npm install no-kafka
```


## Producer

Example:

```javascript
var Kafka = require('no-kafka');
var producer = new Kafka.Producer();

return producer.init().then(function(){
  return producer.send({
      topic: 'kafka-test-topic',
      partition: 0,
      message: {
          value: 'Hello!'
      }
  });
})
.then(function (result) {
  /*
  [ { topic: 'kafka-test-topic', partition: 0, offset: 353 } ]
  */
});
```

Send and retry if failed within 100ms delay:

```javascript
return producer.send(messages, {
  retries: {
    attempts: 2,
    delay: {
      min: 100,
      max: 300
    }
  }
});
```

### Batching (grouping) produce requests

Accumulate messages into single batch until their total size is >= 1024 bytes or 100ms timeout expires (overwrite Producer constructor options):

```javascript
producer.send(messages, {
  batch: {
    size: 1024,
    maxWait: 100
  }
});
producer.send(messages, {
  batch: {
    size: 1024,
    maxWait: 100
  }
});
```

Please note, that if you pass different options to the `send()` method then these messages will be grouped into separate batches:

```javascript
// will be sent in batch 1
producer.send(messages, {
  batch: {
    size: 1024,
    maxWait: 100
  },
  codec: Kafka.COMPRESSION_GZIP
});
// will be sent in batch 2
producer.send(messages, {
  batch: {
    size: 1024,
    maxWait: 100
  },
  codec: Kafka.COMPRESSION_SNAPPY
});
```

### Keyed Messages

Send a message with the key:

```javascript
producer.send({
    topic: 'kafka-test-topic',
    partition: 0,
    message: {
        key: 'some-key'
        value: 'Hello!'
    }
});
```

### Custom Partitioner

Example: override the default partitioner with a custom partitioner that only uses a portion of the key.

```javascript
var util  = require('util');
var Kafka = require('no-kafka');

var Producer           = Kafka.Producer;
var DefaultPartitioner = Kafka.DefaultPartitioner;

function MyPartitioner() {
    DefaultPartitioner.apply(this, arguments);
}

util.inherits(MyPartitioner, DefaultPartitioner);

MyPartitioner.prototype.getKey = function getKey(message) {
    return message.key.split('-')[0];
};

var producer = new Producer({
    partitioner : new MyPartitioner()
});

return producer.init().then(function(){
  return producer.send({
      topic: 'kafka-test-topic',
      message: {
          key   : 'namespace-key',
          value : 'Hello!'
      }
  });
});
```

### Producer options:
* `requiredAcks` - require acknoledgments for produce request. If it is 0 the server will not send any response.  If it is 1 (default), the server will wait the data is written to the local log before sending a response. If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more acknowledgements than there are in-sync replicas).
* `timeout` - timeout in ms for produce request
* `clientId` - ID of this client, defaults to 'no-kafka-client'
* `connectionString` - comma delimited list of initial brokers list, defaults to '127.0.0.1:9092'
* `reconnectionDelay` - controls optionally progressive delay between reconnection attempts in case of network error:
  * `min` - minimum delay, used as increment value for next attempts, defaults to 1000ms
  * `max` - maximum delay value, defaults to 1000ms
* `partitioner` - Class instance used to determine topic partition for message. If message already specifies a partition, the partitioner won't be used. The partitioner must inherit from [`Kafka.DefaultPartitioner`](lib/assignment/partitioners/default.js). The `partition` method receives 3 arguments: the topic name, an array with topic partitions, and the message (useful to partition by key, etc.). `partition` can be sync or async (return a Promise).
* `retries` - controls number of attempts at delay between them when produce request fails
  * `attempts` - number of total attempts to send the message, defaults to 3
  * `delay` - controls delay between retries, the delay is progressive and incrememented with each attempt with `min` value steps up to but not exceeding `max` value
    * `min` - minimum delay, used as increment value for next attempts, defaults to 1000ms
    * `max` - maximum delay value, defaults to 3000ms
* `codec` - compression codec, one of Kafka.COMPRESSION_NONE, Kafka.COMPRESSION_SNAPPY, Kafka.COMPRESSION_GZIP
* `batch` - control batching (grouping) of requests
  * `size` - group messages together into single batch until their total size exceeds this value, defaults to 16384 bytes. Set to 0 to disable batching.
  * `maxWait` - send grouped messages after this amount of milliseconds expire even if their total size doesn't exceed `batch.size` yet, defaults to 10ms. Set to 0 to disable batching.
* `asyncCompression` - boolean, use asynchronouse compression instead of synchronous, defaults to `false`
* `connectionTimeout` - timeout for establishing connection to Kafka in milliseconds, defaults to 3000ms
* `socketTimeout` - timeout for Kafka connection socket in milliseconds, defaults to 0 (disabled)

## SimpleConsumer

Manually specify topic, partition and offset when subscribing. Suitable for simple use cases.

Example:

```javascript
var consumer = new Kafka.SimpleConsumer();

// data handler function can return a Promise
var dataHandler = function (messageSet, topic, partition) {
    messageSet.forEach(function (m) {
        console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
    });
};

return consumer.init().then(function () {
    // Subscribe partitons 0 and 1 in a topic:
    return consumer.subscribe('kafka-test-topic', [0, 1], dataHandler);
});
```

Subscribe (or change subscription) to specific offset and limit maximum received MessageSet size:

```javascript
consumer.subscribe('kafka-test-topic', 0, {offset: 20, maxBytes: 30}, dataHandler)
```

Subscribe to latest or earliest offsets in the topic/parition:

```javascript
consumer.subscribe('kafka-test-topic', 0, {time: Kafka.LATEST_OFFSET}, dataHandler)
consumer.subscribe('kafka-test-topic', 0, {time: Kafka.EARLIEST_OFFSET}, dataHandler)
```

Subscribe to all partitions in a topic:

```javascript
consumer.subscribe('kafka-test-topic', dataHandler)
```

Commit offset(s) (V0, Kafka saves these commits to Zookeeper)

```javascript
consumer.commitOffset([
  {
      topic: 'kafka-test-topic',
      partition: 0,
      offset: 1
  },
  {
      topic: 'kafka-test-topic',
      partition: 1,
      offset: 2
  }
])
```

Fetch commited offset(s)

```javascript
consumer.fetchOffset([
  {
      topic: 'kafka-test-topic',
      partition: 0
  },
  {
      topic: 'kafka-test-topic',
      partition: 1
  }
]).then(function (result) {
/*
[ { topic: 'kafka-test-topic',
    partition: 1,
    offset: 2,
    metadata: null,
    error: null },
  { topic: 'kafka-test-topic',
    partition: 0,
    offset: 1,
    metadata: null,
    error: null } ]
*/
});
```

### SimpleConsumer options
* `groupId` - group ID for comitting and fetching offsets. Defaults to 'no-kafka-group-v0'
* `maxWaitTime` - maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the fetch request is issued, defaults to 100ms
* `idleTimeout` - timeout between fetch calls, defaults to 1000ms
* `minBytes` - minimum number of bytes to wait from Kafka before returning the fetch call, defaults to 1 byte
* `maxBytes` - maximum size of messages in a fetch response, defaults to 1MB
* `clientId` - ID of this client, defaults to 'no-kafka-client'
* `connectionString` - comma delimited list of initial brokers list, defaults to '127.0.0.1:9092'
* `reconnectionDelay` - controls optionally progressive delay between reconnection attempts in case of network error:
  * `min` - minimum delay, used as increment value for next attempts, defaults to 1000ms
  * `max` - maximum delay value, defaults to 1000ms
* `recoveryOffset` - recovery position (time) which will used to recover subscription in case of OffsetOutOfRange error, defaults to Kafka.LATEST_OFFSET
* `asyncCompression` - boolean, use asynchronouse decompression instead of synchronous, defaults to `false`
* `handlerConcurrency` - specify concurrency level for the consumer handler function, defaults to 10
* `connectionTimeout` - timeout for establishing connection to Kafka in milliseconds, defaults to 3000ms
* `socketTimeout` - timeout for Kafka connection socket in milliseconds, defaults to 0 (disabled)

## GroupConsumer (new unified consumer API)

Specify an assignment strategy (or use __no-kafka__ built-in consistent or round robin assignment strategy) and subscribe by specifying only topics. Elected group leader will automatically assign partitions between all group members.

Example:

```javascript
var Promise = require('bluebird');
var consumer = new Kafka.GroupConsumer();

var dataHandler = function (messageSet, topic, partition) {
    return Promise.each(messageSet, function (m){
        console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
        // commit offset
        return consumer.commitOffset({topic: topic, partition: partition, offset: m.offset, metadata: 'optional'});
    });
};

var strategies = [{
    subscriptions: ['kafka-test-topic'],
    handler: dataHandler
}];

consumer.init(strategies); // all done, now wait for messages in dataHandler
```

### Assignment strategies

__no-kafka__ provides three built-in strategies:
* `Kafka.WeightedRoundRobinAssignmentStrategy` weighted round robin assignment (based on [wrr-pool](https://github.com/oleksiyk/wrr-pool)).
* `Kafka.ConsistentAssignmentStrategy` which is based on a consistent [hash ring](https://github.com/3rd-Eden/node-hashring) and so provides consistent assignment across consumers in a group based on supplied `metadata.id` and `metadata.weight` options.
* `Kafka.DefaultAssignmentStrategy` simple round robin assignment strategy (default).

Using `Kafka.WeightedRoundRobinAssignmentStrategy`:

```javascript
var strategies = {
    subscriptions: ['kafka-test-topic'],
    metadata: {
        weight: 4
    },
    strategy: new Kafka.WeightedRoundRobinAssignmentStrategy(),
    handler: dataHandler
};
// consumer.init(strategies)....
```

Using `Kafka.ConsistentAssignmentStrategy`:

```javascript
var strategies = {
    subscriptions: ['kafka-test-topic'],
    metadata: {
        id: process.argv[2] || 'consumer_1',
        weight: 50
    },
    strategy: new Kafka.ConsistentAssignmentStrategy(),
    handler: dataHandler
};
// consumer.init(strategies)....
```
Note that each consumer in a group should have its own and consistent metadata.id.

You can also write your own assignment strategy by inheriting from Kafka.DefaultAssignmentStrategy and overwriting `assignment` method.

### GroupConsumer options

* `groupId` - group ID for comitting and fetching offsets. Defaults to 'no-kafka-group-v0.9'
* `maxWaitTime` - maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the fetch request is issued, defaults to 100ms
* `idleTimeout` - timeout between fetch calls, defaults to 1000ms
* `minBytes` - minimum number of bytes to wait from Kafka before returning the fetch call, defaults to 1 byte
* `maxBytes` - maximum size of messages in a fetch response
* `clientId` - ID of this client, defaults to 'no-kafka-client'
* `connectionString` - comma delimited list of initial brokers list, defaults to '127.0.0.1:9092'
* `reconnectionDelay` - controls optionally progressive delay between reconnection attempts in case of network error:
  * `min` - minimum delay, used as increment value for next attempts, defaults to 1000ms
  * `max` - maximum delay value, defaults to 1000ms
* `sessionTimeout` - session timeout in ms, min 6000, max 30000, defaults to `15000`
* `heartbeatTimeout` - delay between heartbeat requests in ms, defaults to `1000`
* `retentionTime` - offset retention time in ms, defaults to 1 day (24 * 3600 * 1000)
* `startingOffset` - starting position (time) when there is no commited offset, defaults to `Kafka.LATEST_OFFSET`
* `recoveryOffset` - recovery position (time) which will used to recover subscription in case of OffsetOutOfRange error, defaults to Kafka.LATEST_OFFSET
* `asyncCompression` - boolean, use asynchronouse decompression instead of synchronous, defaults to `false`
* `handlerConcurrency` - specify concurrency level for the consumer handler function, defaults to 10
* `connectionTimeout` - timeout for establishing connection to Kafka in milliseconds, defaults to 3000ms
* `socketTimeout` - timeout for Kafka connection socket in milliseconds, defaults to 0 (disabled)

## GroupAdmin (consumer groups API)

Offers methods:

* `listGroups` - list existing consumer groups
* `describeGroup` - describe existing group by its id
* `fetchConsumerLag` - fetches consumer lag for topics/partitions

listGroups, describeGroup:

```javascript
var admin = new Kafka.GroupAdmin();

return admin.init().then(function(){
    return admin.listGroups().then(function(groups){
        // [ { groupId: 'no-kafka-admin-test-group', protocolType: 'consumer' } ]
        return admin.describeGroup('no-kafka-admin-test-group').then(function(group){
            /*
            { error: null,
              groupId: 'no-kafka-admin-test-group',
              state: 'Stable',
              protocolType: 'consumer',
              protocol: 'DefaultAssignmentStrategy',
              members:
               [ { memberId: 'group-consumer-82646843-b4b8-4e91-94c9-b4708c8b05e8',
                   clientId: 'group-consumer',
                   clientHost: '/192.168.1.4',
                   version: 0,
                   subscriptions: [ 'kafka-test-topic'],
                   metadata: <Buffer 63 6f 6e 73 75 6d 65 72 2d 6d 65 74 61 64 61 74 61>,
                   memberAssignment:
                    { _blength: 44,
                      version: 0,
                      partitionAssignment:
                       [ { topic: 'kafka-test-topic',
                           partitions: [ 0, 1, 2 ] },
                          ],
                      metadata: null } },
                  ] }
             */
        })
    });
});
```

fetchConsumerLag:
```javascript
var admin = new Kafka.GroupAdmin();

return admin.init().then(function(){
    return admin.fetchConsumerLag('no-kafka-admin-test-group', [{
        topicName: 'kafka-test-topic',
        partitions: [0, 1, 2]
    }]).then(function (consumerLag) {
        /*
        [ { topic: 'kafka-test-topic',
            partition: 0,
            offset: 11300,
            highwaterMark: 11318,
            consumerLag: 18 },
          { topic: 'kafka-test-topic',
            partition: 1,
            offset: 10380,
            highwaterMark: 10380,
            consumerLag: 0 },
          { topic: 'kafka-test-topic',
            partition: 2,
            offset: -1,
            highwaterMark: 10435,
            consumerLag: null } ]
         */
    });
});
```

Note that group consumer has to commit offsets first, in order for consumerLag to be available. Otherwise the offset will be set to -1.

## Compression

__no-kafka__ supports both SNAPPY and Gzip compression. To use SNAPPY you must install the `snappy` NPM module in your project.

Enable compression in Producer:

```javascript
var Kafka = require('no-kafka');

var producer = new Kafka.Producer({
    clientId: 'producer',
    codec: Kafka.COMPRESSION_SNAPPY // Kafka.COMPRESSION_NONE, Kafka.COMPRESSION_SNAPPY, Kafka.COMPRESSION_GZIP
});
```

Alternatively just send some messages with specified compression codec (overwrites codec set in contructor):

```javascript
return producer.send({
    topic: 'kafka-test-topic',
    partition: 0,
    message: { value: 'p00' }
}, { codec: Kafka.COMPRESSION_SNAPPY })
```

By default __no-kafka__ will use asynchronous compression and decompression.
Disable async compression/decompression (and use sync) with `asyncCompression` option (synchronous Gzip is not availble in node < 0.11):

Producer:

```javascript
var producer = new Kafka.Producer({
    clientId: 'producer',
    asyncCompression: false, // use sync compression/decompression
    codec: Kafka.COMPRESSION_SNAPPY
});
```

Consumer:

```javascript
var consumer = new Kafka.SimpleConsumer({
    idleTimeout: 100,
    clientId: 'simple-consumer',
    asyncCompression: true
});
```

## Connection

### Initial Brokers

__no-kafka__ will connect to the hosts specified in `connectionString` constructor option unless it is omitted. In this case it will use KAFKA_URL environment variable or fallback to default `kafka://127.0.0.1:9092`. For better availability always specify several initial brokers: `10.0.1.1:9092,10.0.1.2:9092,10.0.1.3:9092`. The `/` prefix is optional.

### Disconnect / Timeout Handling
All network errors are handled by the library: producer will retry sending failed messages for configured amount of times, simple consumer and group consumer will try to reconnect to failed host, update metadata as needed as so on.

### SSL
To connect to Kafka with [SSL endpoint enabled](http://kafka.apache.org/090/documentation.html#security_ssl) specify SSL certificate and key options to load cert/key from files or provide certificate/key directly as strings:

Loading certificate and key from file:

```javascript
var producer = new Kafka.Producer({
  connectionString: 'kafka://127.0.0.1:9093', // should match `listeners` SSL option in Kafka config
  ssl: {
    cert: '/path/to/client.crt',
    key: '/path/to/client.key'
  }
});
```

Specifying certificate and key directly as strings:

```javascript
var producer = new Kafka.Producer({
  connectionString: 'kafka://127.0.0.1:9093', // should match `listeners` SSL option in Kafka config
  ssl: {
    cert: '-----BEGIN CERTIFICATE-----\nMIIChTCCAe4C...............',
    key: '-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBA.......'
  }
});
```

Other Node.js SSL options are available such as `rejectUnauthorized`, `secureProtocol`, `ciphers`, etc. See Node.js `tls.createServer` method documentation for more details.

It is also possible to use `KAFKA_CLIENT_CERT` and `KAFKA_CLIENT_CERT_KEY` environment variables to specify SSL certificate and key:

```bash
KAFKA_URL=kafka://127.0.0.1:9093 KAFKA_CLIENT_CERT=./test/ssl/client.crt KAFKA_CLIENT_CERT_KEY=./test/ssl/client.key node producer.js
```

Or as text strings:

```bash
KAFKA_URL=kafka://127.0.0.1:9093 KAFKA_CLIENT_CERT=`cat ./test/ssl/client.crt` KAFKA_CLIENT_CERT_KEY=`cat ./test/ssl/client.key` node producer.js
```

Using a self signed certificate:

```javascript
Kafka.Producer({
  connectionString: 'kafka://127.0.0.1:9093', // should match `listeners` SSL option in Kafka config
  ssl: {
    ca: '/path/to/my-cert.crt' // or fs.readFileSync('my-cert.crt')
  }
});
```

It is also possible to use `KAFKA_CLIENT_CA` environment variable to specify a self signed SSL certificate:

```bash
KAFKA_URL=kafka://127.0.0.1:9093 KAFKA_CLIENT_CA=./test/ssl/my-cert.crt node producer.js
```

### Remapping Broker Addresses
Sometimes the advertised listener addresses for a Kafka cluster may be incorrect from the client,
such as when a Kafka farm is behind NAT or other network infrastructure. In this scenario it is
possible to pass a `brokerRedirection` option to the `Producer`, `SimpleConsumer` or `GroupConsumer`.

The value of the `brokerDirection` can be either:

  - A function returning a tuple of host (string) and port (integer), such as:

        brokerRedirection: function (host, port) {
            return {
                host: host + '.somesuffix.com', // Fully qualify
                port: port + 100,               // Port NAT
            }
        }

  - A simple map of connection strings to new connection strings, such as:

        brokerRedirection: {
            'some-host:9092': 'actual-host:9092',
            'kafka://another-host:9092': 'another-host:9093',
            'third-host:9092': 'kafka://third-host:9000'
        }

A common scenario for this kind of remapping is when a Kafka cluster exists within a Docker application, and the
internally advertised names needed for container to container communication do not correspond to the actual external
ports or addresses when connecting externally via other tools.

### Reconnection delay
In case of network error which prevents further operations __no-kafka__ will try to reconnect to Kafka brokers in a endless loop with the optionally progressive delay which can be configured with `reconnectionDelay` option.

## Logging

You can differentiate messages from several instances of producer/consumer by providing unique `clientId` in options:

```javascript
var consumer1 = new Kafka.GroupConsumer({
    clientId: 'group-consumer-1'
});
var consumer2 = new Kafka.GroupConsumer({
    clientId: 'group-consumer-2'
});
```
=>

```
2016-01-12T07:41:57.884Z INFO group-consumer-1 ....
2016-01-12T07:41:57.884Z INFO group-consumer-2 ....
```

Change the logging level:

```javascript
var consumer = new Kafka.GroupConsumer({
    clientId: 'group-consumer',
    logger: {
        logLevel: 1 // 0 - nothing, 1 - just errors, 2 - +warnings, 3 - +info, 4 - +debug, 5 - +trace
    }
});
```

Send log messages to Logstash server(s) via UDP:

```javascript
var consumer = new Kafka.GroupConsumer({
    clientId: 'group-consumer',
    logger: {
        logstash: {
            enabled: true,
            connectionString: '10.0.1.1:9999,10.0.1.2:9999',
            app: 'myApp-kafka-consumer'
        }
    }
});
```

You can overwrite the function that outputs messages to stdout/stderr:

```javascript
var consumer = new Kafka.GroupConsumer({
    clientId: 'group-consumer',
    logger: {
        logFunction: console.log
    }
});
```

## Topic Creation

There is no Kafka API call to create a topic. Kafka supports auto creating of topics when their metadata is first requested (`auto.create.topic` option) but the topic is created with all default parameters, which is useless. There is no way to be notified when the topic has been created, so the library will need to ping the server with some interval. There is also no way to be notified of any error for this operation. For this reason, having no guarantees, __no-kafka__ won't provide topic creation method until there will be a specific Kafka API call to create/manage topics.

## License: [MIT](https://github.com/oleksiyk/kafka/blob/master/LICENSE)

[badge-license]: https://img.shields.io/badge/License-MIT-green.svg
[license]: https://github.com/oleksiyk/kafka/blob/master/LICENSE
[badge-travis]: https://api.travis-ci.org/oleksiyk/kafka.svg?branch=master
[travis]: https://travis-ci.org/oleksiyk/kafka
[badge-coverage]: https://codeclimate.com/github/oleksiyk/kafka/badges/coverage.svg
[coverage]: https://codeclimate.com/github/oleksiyk/kafka/coverage
[badge-david-deps]: https://david-dm.org/oleksiyk/kafka.svg
[david-deps]: https://david-dm.org/oleksiyk/kafka
[badge-david-dev-deps]: https://david-dm.org/oleksiyk/kafka/dev-status.svg
[david-dev-deps]: https://david-dm.org/oleksiyk/kafka#info=devDependencies
[badge-bithound-code]: https://www.bithound.io/github/oleksiyk/kafka/badges/code.svg
[bithound-code]: https://www.bithound.io/github/oleksiyk/kafka
[badge-bithound-overall]: https://www.bithound.io/github/oleksiyk/kafka/badges/score.svg
[bithound-overall]: https://www.bithound.io/github/oleksiyk/kafka
[badge-bithound-deps]: https://www.bithound.io/github/oleksiyk/kafka/badges/dependencies.svg
[bithound-deps]: https://www.bithound.io/github/oleksiyk/kafka/master/dependencies/npm
[badge-bithound-dev-deps]: https://www.bithound.io/github/oleksiyk/kafka/badges/devDependencies.svg
[bithound-dev-deps]: https://www.bithound.io/github/oleksiyk/kafka/master/dependencies/npm
