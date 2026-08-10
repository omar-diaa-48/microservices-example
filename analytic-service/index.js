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
            topics: ["payment-successful", "order-successful", "email-successful"],
            fromBeginning: true
        })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                switch (topic) {
                    case "payment-successful": {
                        const value = message.value.toString()
                        const { userId, cart } = JSON.parse(value)

                        const total = cart.reduce((acc, item) => acc + item.price, 0)

                        console.log(`Analytic consumer: Payment successful: User ${userId} paid ${total}`)
                    }
                        break;

                    case "order-successful": {
                        const value = message.value.toString()
                        const { userId, orderId } = JSON.parse(value)

                        console.log(`Analytic consumer: Order successful: User ${userId} Order ${orderId}`)
                    }
                        break;

                    case "email-successful": {
                        const value = message.value.toString()
                        const { userId } = JSON.parse(value)

                        const total = cart.reduce((acc, item) => acc + item.price, 0)

                        console.log(`Analytic consumer: Email successful: User ${userId}`)
                    }
                        break;

                    default:
                        break;
                }


            }
        })
    } catch (error) {
        console.error(error)
    }
}

run()