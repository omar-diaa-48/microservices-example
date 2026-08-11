import { otelSdk } from "./tracing.js"
import { Kafka, Partitioners } from "kafkajs"
import pino from "pino"

const logger = pino({ name: "order-service" })

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9094,localhost:9095,localhost:9096").split(",")

const kafka = new Kafka({
    clientId: "order-service",
    brokers,
})

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
})

const consumer = kafka.consumer({
    groupId: "order-service",
})

const run = async () => {
    await producer.connect()
    await consumer.connect()
    await consumer.subscribe({
        topic: "payment-successful",
        fromBeginning: false,
    })

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const { userId } = JSON.parse(message.value.toString())

                const orderId = "98765"
                logger.info({ userId, orderId }, "Order created")

                await producer.send({
                    topic: "order-successful",
                    messages: [{ value: JSON.stringify({ userId, orderId }) }],
                })
            } catch (error) {
                // Swallow bad payloads so one poison message can't crash the consumer.
                // TODO: route failures to a dead-letter topic instead of dropping them.
                logger.error({ error }, "Failed to process payment-successful message")
            }
        },
    })

    logger.info("order-service is up and consuming")
}

const shutdown = async (signal) => {
    logger.info({ signal }, "Shutting down order-service")
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
    logger.error({ error }, "Fatal error starting order-service")
    process.exit(1)
})
