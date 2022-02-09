const { Kafka } = require('kafkajs')
const { v4 } = require('uuid')
require("dotenv").config()
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry')


const kafka = new Kafka({
  clientId: 'AppName',
  // brokers: ['0.0.0.0:9092'],
  brokers: [process.env.BROKER_URL1, process.env.BROKER_URL2, process.env.BROKER_URL3]
})


async function sub() {

  const registry = new SchemaRegistry({ host: 'http://localhost:8081/' })
  const consumer = kafka.consumer({ groupId: 'Consumer: ' + v4() })

  await consumer.connect()

  // check if this system still has heartbeat == still alive
  // const { HEARTBEAT } = consumer.events
  // consumer.on(HEARTBEAT, e =>
  //   console.log(`heartbeat ${e.id} at ${e.timestamp} type ${e.type} inside group ${e.payload.groupId}`))

  // const { REQUEST } = consumer.events
  // consumer.on(REQUEST, e =>
  //   console.log(`REQUEST ID ${e.id} at ${e.timestamp} TYPE ${e.type}  
  //   PAYLOAD ${e.payload.broker} ${e.payload.clientId}, ${e.payload.createdAt}, ${e.payload.sentAt}`))

  // {broker, clientId, correlationId, size, createdAt, sentAt, pendingDuration, duration, apiName, apiKey, apiVersion}


  await consumer.subscribe({ topic: 'test-schema', fromBeginning: true })

  let msgCount = 0

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      msgCount++
      const decodedValue = await registry.decode(message.value)
      // console.log({ topic: topic, offset: message.offset, count: msgCount })
      console.log(decodedValue)
    },
  })

}

sub()