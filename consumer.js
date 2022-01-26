const { Kafka } = require('kafkajs')
// const fs = require('fs')

const kafka = new Kafka({
  clientId: 'Football',
  brokers: ['0.0.0.0:9092'],
  // ssl: {
  //   rejectUnauthorized: false,
  //   ca: [fs.readFileSync('/cert/ca.crt', 'utf-8')],
  //   key: fs.readFileSync('/cert/client-key.pem', 'utf-8'),
  //   cert: fs.readFileSync('/cert/client-cert.pem', 'utf-8')
  // },
  // ssl: true,
  // sasl: {
  //   mechanism: 'plain', // scram-sha-256 or scram-sha-512
  //   username: 'my-username',
  //   password: 'my-password'
  // },
  // retry: {
  //   initialRetryTime: 100,
  //   retries: 8
  // },
  // logLevel: logLevel.ERROR,
  connectionTimeout: 30000,
  requestTimeout: 25000
})


async function main() {

  const consumer = kafka.consumer({ groupId: 'Football' })

  await consumer.connect()

  // check if this system still has heartbeat == still alive
  // const { HEARTBEAT } = consumer.events
  // consumer.on(HEARTBEAT, e =>
  //   console.log(`heartbeat ${e.id} at ${e.timestamp} type ${e.type} inside group ${e.payload.groupId}`))


  await consumer.subscribe({ topic: 'EPL', fromBeginning: true })
  await consumer.subscribe({ topic: 'Bundesliga', fromBeginning: true })

  /**
   * Normal consume - eachMessage
   * */ 

  await consumer.run({
    eachMessage: async ({ topic, _, message }) => {
      console.log({
        key: message.key.toString(),
        value: message.value.toString(),
        headers: message.headers,
        topic: topic,
        // partition: partition,
      })
    },
  })


   /**
   * Consume eachBatch
   * */ 

  // await consumer.run({
  //   eachBatchAutoResolve: true,
  //   eachBatch: async ({
  //     batch,
  //     resolveOffset,
  //     heartbeat,
  //     commitOffsetsIfNecessary,
  //     uncommittedOffsets,
  //     isRunning,
  //     isStale,
  //   }) => {
  //     for (let message of batch.messages) {
  //       if (!isRunning() || isStale()) break
  //       console.log({
  //         topic: batch.topic,
  //         partition: batch.partition,
  //         highWatermark: batch.highWatermark,
  //         message: {
  //           offset: message.offset,
  //           key: message.key.toString(),
  //           value: message.value.toString(),
  //           headers: message.headers,
  //         }
  //       })

  //       // isRunning()

  //       resolveOffset(message.offset)
  //       await heartbeat()
  //     }
  //   },
  // })

  /**
   * Normal consume -  Partition-aware concurrency
   * */ 

  // await consumer.run({
  //   partitionsConsumedConcurrently: 3, // Default: 1
  //   eachMessage: async ({ topic, partition, message }) => {
  //     // This will be called up to 3 times concurrently
  //     try {
  //       await sendToDependency(message)
  //     } catch (e) {
  //       if (e instanceof TooManyRequestsError) {
  //         consumer.pause([{ topic, partitions: [partition] }])
  //         // Other partitions will keep fetching and processing, until if / when
  //         // they also get throttled
  //         setTimeout(() => {
  //           consumer.resume([{ topic, partitions: [partition] }])
  //           // Other partitions that are paused will continue to be paused
  //         }, e.retryAfter * 1000)
  //       }

  //       throw e
  //     }
  //   },
  // })

}

main()