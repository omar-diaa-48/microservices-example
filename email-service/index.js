import { otelSdk } from "./tracing.js"
import { Kafka, Partitioners } from "kafkajs"
import pino from "pino"

const logger = pino({ name: "email-service" })

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9094,localhost:9095,localhost:9096").split(",")

const kafka = new Kafka({
    clientId: "email-service",
    brokers,
})

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    // Idempotent producer: acks=all + max-in-flight=1, so a retried batch can
    // never be reordered behind a later one within a partition.
    idempotent: true,
})

const consumer = kafka.consumer({
    groupId: "email-service",
})

const run = async () => {
    await producer.connect()
    await consumer.connect()
    await consumer.subscribe({
        topic: "order-successful",
        fromBeginning: false,
    })

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const { userId } = JSON.parse(message.value.toString())

                logger.info({ userId }, "Email sent")

                await producer.send({
                    topic: "email-successful",
                    // key = userId → keeps this user's saga events on one partition, in order.
                    messages: [{ key: userId, value: JSON.stringify({ userId }) }],
                })
            } catch (error) {
                // Swallow bad payloads so one poison message can't crash the consumer.
                // TODO: route failures to a dead-letter topic instead of dropping them.
                logger.error({ error }, "Failed to process order-successful message")
            }
        },
    })

    logger.info("email-service is up and consuming")
}

const shutdown = async (signal) => {
    logger.info({ signal }, "Shutting down email-service")
    try {
        await consumer.disconnect()
        await producer.disconnect()
    } catch (error) {
        logger.error({ error }, "Error during Kafka disconnect")
    }
    try {
        await otelSdk.shutdown()
    } catch (error) {
        logger.error({ error }, "Error during OpenTelemetry shutdown")
    }
    process.exit(0)
}

process.on("SIGTERM", () => shutdown("SIGTERM"))
process.on("SIGINT", () => shutdown("SIGINT"))

run().catch((error) => {
    logger.error({ error }, "Fatal error starting email-service")
    process.exit(1)
})
