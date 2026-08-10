import { Kafka, Partitioners } from "kafkajs"

const kafka = new Kafka({
    clientId: "analytic-service",
    brokers: ["localhost:9094"]
})

const consumer = kafka.consumer({
    createPartitioner: Partitioners.DefaultPartitioner,
    groupId: "analytic-service"
})

const run = async () => {
    try {
        await consumer.connect()
        await consumer.subscribe({
            topic: "payment-successful",
            fromBeginning: true
        })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString()
                const { userId, cart } = JSON.parse(value)

                const total = cart.reduce((acc, item) => acc + item.price, 0)

                console.log(`Analytic consumer: User ${userId} paid ${total}`)
            }
        })
    } catch (error) {
        console.error(error)
    }
}

run()