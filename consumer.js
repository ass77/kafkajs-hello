const { Kafka } = require('kafkajs')
const { v4 } = require('uuid')
require("dotenv").config()
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry')


const kafka = new Kafka({
  clientId: 'GenesisApp',
  // brokers: ['0.0.0.0:9092'],
  brokers: [process.env.BROKER_URL1, process.env.BROKER_URL2, process.env.BROKER_URL3]
})


async function sub() {

  const registry = new SchemaRegistry({ host: process.env.SCHEMA_URL2 })
  // Always connect to LEADER first

  const consumer = kafka.consumer({ groupId: v4() })

  await consumer.connect()

  await consumer.subscribe({ topic: 'site-action-D', fromBeginning: true })


  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const decodedValue = await registry.decode(message.value)
      const payload =
      {
        key: message.key.toString(),
        topic: topic,
        payload: decodedValue,
        partition: partition,
        offset: message.offset,
        timestamp: message.timestamp,
      }

      console.log(payload)
    },
  })

}

sub()