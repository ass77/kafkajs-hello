const { Kafka } = require('kafkajs')

const client = new Kafka({
    clientId: 'Football',
    brokers: ['0.0.0.0:9092'],
})


async function main() {

    // IDK : how to get transaction ID
    const producer = client.producer({ maxInFlightRequests: 1, idempotent: true })

    const transaction = await producer.transaction()

    // const topics = [
    //     {
    //         topic: "foo",
    //         partitions: [{
    //             partition: 1,
    //             offset: 0,
    //         }]
    //     }
    // ]

    let topic = "EPL"
    let bets = "$100,000 bets Newcastle United will not be able to win the Premier League anymore"

    let messages =
    {
        key: 'div1',
        value: bets,
        partition: 0,
    }

    let topics = [{
        topic: "Bundesliga offset",
        partitions: [{
            partition: 0,
            offset: 42,
        }]
    }]

    try {
        await transaction.send({ topic, messages })

        // await transaction.sendOffsets({
        //     consumerGroupId, topics
        // })

        await transaction.commit()

    } catch (e) {

        await transaction.abort()

    }
}

main()