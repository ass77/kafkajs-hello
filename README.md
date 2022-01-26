# Introduction to Apache KafkaJS

Kafka is a messaging system that safely moves data between systems. Depending on how each component is configured, it can act as a transport for real-time event tracking or as a replicated distributed database. Although it is commonly referred to as a queue, it is more accurate to say that it is something in between a queue and a database, with attributes and tradeoffs from both types of systems.

keyword: `data-streaming-platform`

### Roles and Usage

`NOTE: broker is our own local-machine 127.0.0.1:9092 (kafka instance) after installing redpanda rpk cli and start the services via systemctl start redpanda`

a. Create group (clientId) : `auto set @ consumer.js at line31`

b. Create topic with two partitions : `rpk topic create EPL -p 2 && rpk topic create Bundesliga -p 2`

1. Admin: `hosts all clusters operation (CRUD) such as createTopic, createPartitions, createACL, deleteTopic etc.`
```
node admin.js
```

2. Consumer(read): `consume any data that produced from the producer`

```
node consumer.js
```

3. Producer(write): `transmitting data to broker, consumer subscribe to its respective channel (group + topic) and listen to new data/events that is coming`
```
node producer.js
```