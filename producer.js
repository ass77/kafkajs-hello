const { Kafka } = require('kafkajs')
require("dotenv").config()
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry')



const kafka = new Kafka({
  clientId: 'AppName',
  // brokers: ['0.0.0.0:9092'],
  brokers: [process.env.BROKER_URL1,process.env.BROKER_URL2,process.env.BROKER_URL3]

})

async function pub() {

  const schema = {
    name: "animalist",
    namespace: "binatang",
    type: "record",
    fields: [
      { name: "name", type: "string" },
      { name: "breed", type: "string" },
      { name: "color", type: "string" },
    ],
  };

  const registry = new SchemaRegistry({ host: 'http://localhost:8081' })

  const { id } = await registry.register({
    type: SchemaType.AVRO,
    schema: JSON.stringify(schema),
  });


  const producer = kafka.producer()

  await producer.connect()

  // Encode using the uploaded schema
  const payload = { name: "Charlie", breed: "Golden Retrievers", color: "Gold" };
  const encodedValue = await registry.encode(id, payload);

  await producer.send({
    topic: 'test-schema',
    messages: [{ value: encodedValue }],
  })

  await producer.disconnect()
}

pub()