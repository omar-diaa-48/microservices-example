import { Kafka } from "kafkajs"

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9094,localhost:9095,localhost:9096").split(",")

const kafka = new Kafka({
    clientId: "kafka-admin",
    brokers,
})

const admin = kafka.admin()

const run = async () => {
    await admin.connect()
    const created = await admin.createTopics({
        // false = do not error/hang if a topic already exists (idempotent init)
        topics: [
            { topic: "payment-successful" },
            { topic: "order-successful" },
            { topic: "email-successful" },
        ],
    })
    console.log(created ? "Topics created" : "Topics already exist — nothing to do")
    await admin.disconnect()
}

run()
    .then(() => process.exit(0))
    .catch(async (error) => {
        console.error("Failed to initialise Kafka topics", error)
        try {
            await admin.disconnect()
        } catch { }
        process.exit(1)
    })
