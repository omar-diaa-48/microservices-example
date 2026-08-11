import { otelSdk } from "./tracing.js"
import { Kafka } from "kafkajs"
import pino from "pino"

const logger = pino({ name: "analytic-service" })

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9094,localhost:9095,localhost:9096").split(",")

const kafka = new Kafka({
    clientId: "analytic-service",
    brokers,
})

const consumer = kafka.consumer({
    groupId: "analytic-service",
})

const run = async () => {
    await consumer.connect()
    await consumer.subscribe({
        topics: ["payment-successful", "order-successful", "email-successful"],
        fromBeginning: false,
    })

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            try {
                const payload = JSON.parse(message.value.toString())

                switch (topic) {
                    case "payment-successful": {
                        const { userId, cart } = payload
                        const total = cart.reduce((acc, item) => acc + item.price, 0)
                        logger.info({ userId, total }, "Analytic: payment successful")
                        break
                    }
                    case "order-successful": {
                        const { userId, orderId } = payload
                        logger.info({ userId, orderId }, "Analytic: order successful")
                        break
                    }
                    case "email-successful": {
                        const { userId } = payload
                        logger.info({ userId }, "Analytic: email successful")
                        break
                    }
                    default:
                        break
                }
            } catch (error) {
                // Swallow bad payloads so one poison message can't crash the consumer.
                // TODO: route failures to a dead-letter topic instead of dropping them.
                logger.error({ error, topic }, "Failed to process message")
            }
        },
    })

    logger.info("analytic-service is up and consuming")
}

const shutdown = async (signal) => {
    logger.info({ signal }, "Shutting down analytic-service")
    try {
        await consumer.disconnect()
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
    logger.error({ error }, "Fatal error starting analytic-service")
    process.exit(1)
})
