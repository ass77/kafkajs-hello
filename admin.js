const { Kafka, ConfigResourceTypes } = require('kafkajs')
const {
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('kafkajs')

const kafka = new Kafka({
  clientId: 'Football',
  brokers: ['0.0.0.0:9092', 'kafka2:9092'],
  connectionTimeout: 3000,
  requestTimeout: 20000
})

const admin = kafka.admin()

async function listTopicMetadata() {
  await admin.connect()

  const result = await admin.listTopics()

  console.log(result)

  const metadata = await admin.fetchTopicMetadata({
    topics: ['EPL', 'Bundesliga'],
  })

  for (let i = 0; i < metadata.topics.length; i++) {
    console.log(metadata.topics[i].partitions, `metadata of partitions ${metadata.topics[i].name}`)
  }


  const topic = "EPL"

  const offset = await admin.fetchTopicOffsets(topic)

  console.log(offset, 'offset')

  const groupId = "Football"

  // await admin.resetOffsets({ groupId, topic })
  // const groupOffset =await admin.fetchOffsets({ groupId, topic, resolveOffsets: true })
  const groupOffset = await admin.fetchOffsets({ groupId, topic, resolveOffsets: false })

  console.log(groupOffset, `group ${groupId} offset`)

  await admin.disconnect()

}

listTopicMetadata()

async function setGroupOffset() {
  await admin.connect()

  await admin.setOffsets({
    groupId: 'Football',
    topic: 'EPL',
    partitions: [
      { partition: 0, offset: '35' },
      { partition: 1, offset: '19' },
    ]
  })

  let groupId = "Football"

  let topic = "EPL"

  let timestamp = Date.now()

  await admin.setOffsets({ groupId, topic, partitions: await admin.fetchTopicOffsetsByTimestamp(topic, timestamp) })


  await admin.disconnect()
}

// setGroupOffset()

async function createTopic() {

  await admin.connect()
  await admin.createTopics({
    validateOnly: false,
    waitForLeaders: false,
    timeout: 25000,
    topics: [{
      topic: "topic",
      numPartitions: 2,     // default: 1
      configEntries: [{ name: 'cleanup.policy', value: 'compact' }]
    },
    {
      topic: "Bundesliga",
      numPartitions: 1,
      configEntries: [{ name: 'cleanup.policy', value: 'compact' }]
    }],
  })

  await admin.disconnect()

}

// createTopic()


async function deleteTopic() {
  await admin.connect()

  await admin.deleteTopics({
    topics: ["Bundesliga"],
    timeout: 5000,
  })

  await admin.disconnect()
}

// deleteTopic()

async function createPartitions() {
  await admin.connect()

  await admin.createPartitions({
    validateOnly: false,
    timeout: 3000,
    topicPartitions: [{
      topic: "topic",
      count: 2,
      assignments: [[0, 1], [1, 2]] // Example: [[0,1],[1,2],[2,0]]
    }],
  })

  await admin.disconnect()
}

// createPartitions()

async function clusterConfig() {

  await admin.connect()

  const cluster = await admin.describeCluster()
  console.log(cluster, 'cluster')

  const allConfigs = await admin.describeConfigs({
    includeSynonyms: false,
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: 'topic',
        // configNames: ['cleanup.policy']
      }
    ]
  })

  console.log(allConfigs, 'allConfigs topic')

  const listGroup = await admin.listGroups()
  console.log(listGroup, 'listGroup')

  const describeGroup = await admin.describeGroups(['Football'])

  console.log(describeGroup, 'describe group')

  await admin.disconnect()

}

// clusterConfig()

async function alterConfig() {

  await admin.connect()

  await admin.alterConfigs({
    resources: [{
      type: ConfigResourceTypes.TOPIC,
      name: 'Bundesliga',
      configEntries: [
        { name: 'cleanup.policy', value: 'compact' },
      ]
    }]
  })

  await admin.disconnect()
}

// alterConfig()

async function deleteGroup() {
  await admin.connect()

  let group_name = "5063057d-7e3e-43d3-ab80-03ff27c8f424"

  console.log(`deleting ${group_name} ...`)

  await admin.deleteGroups([group_name])

  console.log(`successfully deleted ${group_name} ...`)


  await admin.disconnect()
}

// deleteGroup()
// 
async function deleteTopic() {
  await admin.connect()
  // 
  console.log('111111111111111111111')
  // 
  await admin.deleteTopicRecords({
    topic: 'testHello',
    partitions: [
      { partition: 0, offset: -1 }, // delete up to and including offset 29
    ]
  })
  // 
  console.log('2222222222222222222222222')
  // 
  // 
  await admin.disconnect()
}
// 
// deleteTopic()


async function createACL() {
  await admin.connect()

  const acl = [
    {
      resourceType: AclResourceTypes.TOPIC,
      resourceName: 'topic-name',
      resourcePatternType: ResourcePatternTypes.LITERAL,
      principal: 'User:bob',
      host: '*',
      operation: AclOperationTypes.ALL,
      permissionType: AclPermissionTypes.DENY,
    },
    {
      resourceType: AclResourceTypes.TOPIC,
      resourceName: 'topic-name',
      resourcePatternType: ResourcePatternTypes.LITERAL,
      principal: 'User:alice',
      host: '*',
      operation: AclOperationTypes.ALL,
      permissionType: AclPermissionTypes.ALLOW,
    },
  ]

  await admin.createAcls({ acl })

  await admin.disconnect()
}

// ACL()

async function deleteACL() {
  await admin.connect()

  const acl = {
    resourceName: 'Rtopic-name',
    resourceType: AclResourceTypes.TOPIC,
    host: '*',
    permissionType: AclPermissionTypes.ALLOW,
    operation: AclOperationTypes.ANY,
    resourcePatternType: ResourcePatternTypes.LITERAL,
  }

  await admin.deleteAcls({ filters: [acl] })

  await admin.disconnect()
}

// deleteACL()

async function descACL() {
  await admin.connect()

  await admin.describeAcls({
    resourceName: 'Rtopic-name',
    resourceType: AclResourceTypes.TOPIC,
    host: '*',
    permissionType: AclPermissionTypes.ALLOW,
    operation: AclOperationTypes.ANY,
    resourcePatternTypeFilter: ResourcePatternTypes.LITERAL,
  })
  // {
  //   resources: [
  //     {
  //       resourceType: AclResourceTypes.TOPIC,
  //       resourceName: 'topic-name,
  //       resourcePatternType: ResourcePatternTypes.LITERAL,
  //       acls: [
  //         {
  //           principal: 'User:alice',
  //           host: '*',
  //           operation: AclOperationTypes.ALL,
  //           permissionType: AclPermissionTypes.ALLOW,
  //         },
  //       ],
  //     },
  //   ],
  // }

  await admin.disconnect()

}

// descACL()