const { Kafka } = require('kafkajs')
require("dotenv").config()


const kafka = new Kafka({
  clientId: 'AppName',
  brokers: ['0.0.0.0:9092'],
  // brokers: ["process.env.BROKER_URL1","process.env.BROKER_URL2","process.env.BROKER_URL3"]

})


async function pub() {


  const producer = kafka.producer()

  await producer.connect()


  const topicMessages = [
    {
      topic: 'EPL',
      messages: [
        {
          key: 'key1',
          value: 'AppName1',
          // partition: 0
        },
        {
          key: 'key2',
          value: 'AppName2',
          // partition: 1
        }
      ],
    }]


  await producer.sendBatch({topicMessages})

  await producer.disconnect()
}

pub()