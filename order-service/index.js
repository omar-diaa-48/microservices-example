import { Kafka, Partitioners } from "kafkajs"

const kafka = new Kafka({
    clientId: "order-service",
    brokers: ["localhost:9094","localhost:9095","localhost:9096"]
})

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
})

const consumer = kafka.consumer({
    groupId: "order-service"
})

const run = async () => {
    try {
        await producer.connect()
        await consumer.connect()
        await consumer.subscribe({
            topic: "payment-successful",
            fromBeginning: false
        })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString()
                const { userId, cart } = JSON.parse(value)

                const orderId = "98765"
                console.log(`Order for user ${userId} is created with order ID ${orderId}`)

                await producer.send({
                    topic: "order-successful",
                    messages: [
                        { value: JSON.stringify({ userId, orderId }) }
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