# node-kafka

Node.js binding for [librdkafka](https://github.com/edenhill/librdkafka).

Only connect and produce are implemented so far.  consume will be forthcoming (but no immediate need)

## KAFKA SETUP
From kafka folder

### start servers
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```
### create topic
```bash
bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 1 --topic test
```
### check topic
```bash
bin/kafka-list-topic.sh --zookeeper localhost:2181
```
### consumer
```bash
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
```
### test producer
```bash
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```

## BUILD
### Configure
```bash
node-gyp configure
```

### Initial build
```bash
node-gyp build
```

### Rebuild
```bash
node-gyp rebuild
```

## TEST
```bash
node example.js
node example2.js
```