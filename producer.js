const { Kafka } = require('kafkajs')
require("dotenv").config()
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry')

const kafka = new Kafka({
  clientId: 'AppName',
  // brokers: ['0.0.0.0:9092'],
  brokers: [process.env.BROKER_URL1, process.env.BROKER_URL2, process.env.BROKER_URL3]

})

const registry = new SchemaRegistry({ host: 'http://localhost:8081' })

async function pub() {
  // AVRO schema type

  // schema registry will be registered under subject 
  const schema = {
    name: "animalist", //for subject purposes
    namespace: "binatang", //for subject purposes
    type: "record",
    fields: [
      { name: "name", type: "string" },
      { name: "breed", type: "string" },
      { name: "color", type: "string" },
    ],
  };
  // subject name = namespace.name

  // JSON schema type

  // const schema = `
  //   {
  //     "definitions" : {
  //       "record:Human.THC" : {
  //         "type" : "object",
  //         "required" : [ "name", "age", "color" ],
  //         "additionalProperties" : false,
  //         "properties" : {
  //           "name" : {
  //             "type" : "string"
  //           },
  //           "age" : {
  //             "type" : "integer"
  //           },
  //           "color" : {
  //             "type" : "string"
  //           }
  //         }
  //       }
  //     },
  //     "$ref" : "#/definitions/record:Human.THC"
  //   }
  // `


  // // TODO where to host this schema registry ? Can we host it @ redpanda brokers ?

  const { id } = await registry.register({
  type: SchemaType.AVRO,
  schema: JSON.stringify(schema),
  });


  // const { id } = await registry.register(
  //   { type: SchemaType.JSON, schema },
  //   { subject: "Human.THC" })

  const producer = kafka.producer()

  await producer.connect()

  // Encode using the uploaded schema

  // AVRO
  const payload = { name: "Charlie", breed: "Golden Retrievers", color: "Gold" };

  // BUG Invalid schema Type JSON
  // const payload = { name: "Meowie", age: 42, color: "White" };

  const encodedValue = await registry.encode(id, payload);

  await producer.send({
    topic: 'buddi',
    messages: [{ value: encodedValue }],
  })

  await producer.disconnect()
}

pub()