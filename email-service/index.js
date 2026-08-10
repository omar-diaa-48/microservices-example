import { Kafka, Partitioners } from "kafkajs"

const kafka = new Kafka({
    clientId: "email-service",
    brokers: ["localhost:9094"]
})

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
})

const consumer = kafka.consumer({
    createPartitioner: Partitioners.DefaultPartitioner,
    groupId: "email-service"
})

const run = async () => {
    try {
        await producer.connect()
        await consumer.connect()
        await consumer.subscribe({
            topic: "order-successful",
            fromBeginning: true
        })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString()
                const { userId, orderId } = JSON.parse(value)

                console.log(`Email sent to user ${userId}`)

                producer.send({
                    topic: "email-successful",
                    messages: [
                        { value: JSON.stringify(userId) }
                    ]
                })
            }
        })
    } catch (error) {
        console.error(error)
    }
}

run()