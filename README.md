[![Build Status](https://travis-ci.org/oleksiyk/kafka.svg)](https://travis-ci.org/oleksiyk/kafka)
[![Dependencies](https://david-dm.org/oleksiyk/kafka.svg)](https://david-dm.org/oleksiyk/kafka)
[![DevDependencies](https://david-dm.org/oleksiyk/kafka/dev-status.svg)](https://david-dm.org/oleksiyk/kafka#info=devDependencies)

# no-kafka

no-kafka is [Apache Kafka](https://kafka.apache.org) 0.9 client for Node.js with [new unified consumer API](#groupconsumer-new-unified-consumer-api) support. No Zookeeper connection required. Doesn't support compression yet.

All methods will return a [promise](https://github.com/petkaantonov/bluebird)

## Using

* download and install Kafka
* create your test topic:
```shell
kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic kafka-test-topic --partitions 3 --replication-factor 1
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

### Producer options:
* `requiredAcks` - require acknoledgments for produce request. If it is 0 the server will not send any response.  If it is 1 (default), the server will wait the data is written to the local log before sending a response. If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more acknowledgements than there are in-sync replicas).
* `timeout` - timeout in ms for produce request
* `clientId` - ID of this client, defaults to 'no-kafka-client'
* `connectionString` - comma delimited list of initial brokers list, defaults to '127.0.0.1:9092'

## SimpleConsumer

Manually specify topic, partition and offset when subscribing. Suitable for simple use cases.

Example:

```javascript
var consumer = new Kafka.SimpleConsumer();

consumer.on('data', function (messageSet, topic, partition) {
    messageSet.forEach(function (m) {
        console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
    });
});

return consumer.init().then(function () {
    return Promise.all([
        consumer.subscribe('kafka-test-topic', 0),
        consumer.subscribe('kafka-test-topic', 1)
    ]);
});
```

Subscribe (or change subscription) to specific offset and limit maximum received MessageSet size:
```javascript
consumer.subscribe('kafka-test-topic', 0, {offset: 20, maxBytes: 30})
```

Subscribe to latest or earliest offsets in the topic/parition:
```javascript
consumer.subscribe('kafka-test-topic', 0, {time: Kafka.LATEST_OFFSET})
consumer.subscribe('kafka-test-topic', 0, {time: Kafka.EARLIEST_OFFSET})
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
* `timeout` - timeout for fetch requests, defaults to 100ms
* `idleTimeout` - timeout between fetch calls, defaults to 1000ms
* `minBytes` - minimum number of bytes to wait from Kafka before returning the fetch call, defaults to 1 byte
* `maxBytes` - maximum size of messages in a fetch response
* `clientId` - ID of this client, defaults to 'no-kafka-client'
* `connectionString` - comma delimited list of initial brokers list, defaults to '127.0.0.1:9092'
* `recoveryOffset` - recovery position (time) which will used to recover subscription in case of OffsetOutOfRange error, defaults to Kafka.LATEST_OFFSET

## GroupConsumer (new unified consumer API)

Specify an assignment strategy (or use no-kafka built-in consistent or round robin assignment strategy) and subscribe by specifying only topics. Elected group leader will automatically assign partitions between all group members.

Example:

```javascript
var consumer = new Kafka.GroupConsumer();
var strategies = [{
    strategy: 'TestStrategy',
    subscriptions: ['kafka-test-topic']
}];

consumer.on('data', function (messageSet, topic, partition) {
    messageSet.forEach(function (m) {
        console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
        // process each message and commit its offset
        consumer.commitOffset({topic: topic, partition: partition, offset: m.offset, metadata: 'optional'});
    });
});

return consumer.init(strategies).then(function(){
  // all done, now wait for messages in event listener
});
```

### Assignment strategies

no-kafka provides three built-in strategies:
* `Kafka.WeightedRoundRobinAssignment` weighted round robin assignment (based on [wrr-pool](https://github.com/oleksiyk/wrr-pool)).
* `Kafka.ConsistentAssignment` which is based on a consistent [hash ring](https://github.com/3rd-Eden/node-hashring) and so provides consistent assignment across consumers in a group based on supplied `metadata.id` and `metadata.weight` options.
* `Kafka.RoundRobinAssignment` simple assignment strategy (default).

Using `Kafka.WeightedRoundRobinAssignment`:
```javascript
var strategies = {
    strategy: 'TestStrategy',
    subscriptions: ['kafka-test-topic'],
    metadata: {
        weight: 4
    },
    fn: Kafka.WeightedRoundRobinAssignment
};
// consumer.init(strategies)....
```

Using `Kafka.ConsistentAssignment`:
```javascript
var strategies = {
    strategy: 'TestStrategy',
    subscriptions: ['kafka-test-topic'],
    metadata: {
        id: process.argv[2] || 'consumer_1',
        weight: 50
    },
    fn: Kafka.ConsistentAssignment
};
// consumer.init(strategies)....
```
Note that each consumer in a group should have its own and consistent metadata.id.

Using `Kafka.RoundRobinAssignment` (default in no-kafka):
```javascript
var strategies = {
    strategy: 'TestStrategy',
    subscriptions: ['kafka-test-topic'],
};
// consumer.init(strategies)....
```

You can also write your own assignment strategy function and provide it as `fn` option of the strategy item.

### GroupConsumer options

* `groupId` - group ID for comitting and fetching offsets. Defaults to 'no-kafka-group-v0.9'
* `timeout` - timeout for fetch requests, defaults to 100ms
* `idleTimeout` - timeout between fetch calls, defaults to 1000ms
* `minBytes` - minimum number of bytes to wait from Kafka before returning the fetch call, defaults to 1 byte
* `maxBytes` - maximum size of messages in a fetch response
* `clientId` - ID of this client, defaults to 'no-kafka-client'
* `connectionString` - comma delimited list of initial brokers list, defaults to '127.0.0.1:9092'
* `sessionTimeout` - session timeout in ms, min 6000, max 30000, defaults to 15000
* `heartbeatTimeout` - delay between heartbeat requests in ms, defaults to 1000
* `retentionTime` - offset retention time in ms, defaults to 1 day (24 * 3600 * 1000)
* `startingOffset` - starting position (time) when there is no commited offset, defaults to Kafka.LATEST_OFFSET
* `recoveryOffset` - recovery position (time) which will used to recover subscription in case of OffsetOutOfRange error, defaults to Kafka.LATEST_OFFSET

## GroupAdmin (consumer groups API)

Offes two methods:

* `listGroups` - list existing consumer groups
* `describeGroup` - describe existing group by its id

Example:

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
              protocol: 'TestStrategy',
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

You can also tune the logging level which should be bitwise OR for the following:

- 1 - errors
- 2 - warnings
- 4 - log (info)
- 16 - debug

```javascript
var consumer = new Kafka.GroupConsumer({
    clientId: 'group-consumer',
    nsl: {
        logLevel: 7 // filter out debug messages (1 | 2 | 4)
    }
});
```

You can overwrite all (error(), log(), warn(), debug()) or just some of the logger functions:

```javascript
var consumer = new Kafka.GroupConsumer({
    clientId: 'group-consumer',
    logger: {
        log: console.log // overwrite just the .log() method
    }
});
```

First argument passed to custom log functions will always be `clientId`:

 ```
 group-consumer Joined group no-kafka-group-v0.9 generationId 1 as group-consumer-c88fa417-b6a2-499c-b03f-fa66093831f6
 ```
