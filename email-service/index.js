import { Kafka, Partitioners } from "kafkajs"

const kafka = new Kafka({
    clientId: "email-service",
    brokers: ["localhost:9094","localhost:9095","localhost:9096"]
})

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
})

const consumer = kafka.consumer({
    groupId: "email-service"
})

const run = async () => {
    try {
        await producer.connect()
        await consumer.connect()
        await consumer.subscribe({
            topic: "order-successful",
            fromBeginning: false
        })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString()
                const { userId, orderId } = JSON.parse(value)

                console.log(`Email sent to user ${userId}`)

                await producer.send({
                    topic: "email-successful",
                    messages: [
                        { value: JSON.stringify({ userId }) }
                    ]
                })
            }
        })
    } catch (error) {
        await producer.disconnect()
        await consumer.disconnect()
        console.error(error)
    }
}

run()