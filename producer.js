const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'Football',
  brokers: ['0.0.0.0:9092'],
  // connectionTimeout: 3000,
  // requestTimeout: 20000
})


async function main() {


  const producer = kafka.producer()

  await producer.connect()


  const topicMessages = [
    {
      topic: 'EPL',
      messages: [
        {
          key: 'div1',
          value: 'mockdata1',
          partition: 0,
          headers: {
            'correlation-id': 'Jack',
          },
        },
        {
          key: 'div2',
          value: 'mocodata2',
          partition: 1,
          headers: {
            'correlation-id': 'Pilip',
          },
        }
      ],
    },
    {
      topic: 'Bundesliga',
      messages: [
        {
          key: 'division1',
          value: 'mock-data1',
          partition: 0,
          headers: {
            'correlation-id': 'terminator',
          },
        },
        {
          key: 'division2',
          value: 'mock-data2',
          partition: 1,
          headers: {
            'correlation-id': 'arnold schwarzenegger',
          },
        }
      ],
    }]


  await producer.sendBatch({
    topicMessages
  })


  await producer.disconnect()
}

main()