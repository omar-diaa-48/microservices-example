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
        //
        // Ordering note: Kafka only orders messages WITHIN a partition. With
        // >1 partition, different keys spread out for throughput, while every
        // message sharing a key stays on one partition and in order. The
        // producers key by userId, so each user's saga events stay ordered.
        // (numPartitions only applies when the topic is first created — run
        // `docker compose down -v` to recreate topics after changing it.)
        topics: [
            { topic: "payment-successful", numPartitions: 3 },
            { topic: "order-successful", numPartitions: 3 },
            { topic: "email-successful", numPartitions: 3 },
            // Compensation (saga rollback) topics:
            { topic: "order-failed", numPartitions: 3 },
            { topic: "payment-refunded", numPartitions: 3 },
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
