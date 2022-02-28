const { Kafka } = require('kafkajs')
require("dotenv").config()
const { v4 } = require('uuid')
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry')

const kafka = new Kafka({
  clientId: 'GenesisApp',
  brokers: [process.env.BROKER_URL1, process.env.BROKER_URL2, process.env.BROKER_URL3],

})

// const kafka = new Kafka({
//   clientId: 'serverless-referral',
//   brokers: [process.env.BROKER_URL1, process.env.BROKER_URL2, process.env.BROKER_URL3],
// })


const registry = new SchemaRegistry({ host: process.env.SCHEMA_URL1 })
// Always connect to LEADER first

async function pub() {
  // AVRO schema type

  // schema registry will be registered under subject and can be viewed in topic
  // const schema = {
  //   name: "jobs", //for subject purposes
  //   namespace: "animal", //for subject purposes
  //   type: "record",
  //   fields: [
  //     { name: "name", type: "string" },
  //     { name: "age", type: "int" },
  //     { name: "job", type: "string" },
  //   ],
  // };
  // // subject name = namespace.name

  // const schema = {
  //   namespace: "testServ", //for subject purposes
  //   name: "fererroT", //for subject purposes
  //   type: "record",
  //   fields: [
  //     { name: "url", type: ['null', 'string'], default: null },
  //     { name: "referrerId", type: ['null', 'string'], default: null },
  //     { name: "createdAt", type: ['null', 'string'], default: null },
  //     { name: "updatedAt", type: ['null', 'string'], default: null },
  //   ],
  // };


  const schema = {
    namespace: "actionType", //for subject purposes
    name: "fererroT", //for subject purposes
    type: "record",
    fields: [
      { name: "actionType", type: ['null', 'string'], default: null },
      { name: "url", type: ['null', 'string'], default: null },
      { name: "referrerId", type: ['null', 'string'], default: null },
      { name: "createdAt", type: ['null', 'string'], default: null },
      { name: "updatedAt", type: ['null', 'string'], default: null },
    ],
  };



  // register & publish schema
  const { id } = await registry.register({
    type: SchemaType.AVRO,
    schema: JSON.stringify(schema),
  });

  const producer = kafka.producer()

  await producer.connect()



  // stress testing 
  // for (let i = 0; i < 2; i++) {

  //   const payload = { name: `Action-${i}`, age: i, job: `Job-${i}` };
  //   const encodedValue = await registry.encode(id, payload);

  //   await producer.send({
  //     topic: "site-action-C",
  //     messages: [{ key: `key-D${i}`, value: encodedValue }],
  //   });

  //   console.log(`sending ${i} msg`)

  // }

  const payload = {
    actionType: 'Secondary',
    url: "https://www.facebook.com",
    referrerId: "690690",
    createdAt: "2020-01-01T00:00:00.000Z",
    updatedAt: "2020-01-01T00:00:00.000Z",
  }

  const encodedValue = await registry.encode(id, payload);

  await producer.send({
    topic: 'site-action-B',
    messages: [{ key: 'TEST', value: encodedValue }],
  })

  await producer.disconnect()
}

pub()