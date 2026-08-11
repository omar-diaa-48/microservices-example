import { Kafka, Partitioners } from "kafkajs"
import pino from "pino"
import { otelSdk } from "./tracing.js"

const logger = pino({ name: "order-service" })

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9094,localhost:9095,localhost:9096").split(",")

const kafka = new Kafka({
    clientId: "order-service",
    brokers,
})

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    // Idempotent producer: acks=all + max-in-flight=1, so a retried batch can
    // never be reordered behind a later one within a partition.
    idempotent: true,
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
                const { userId, cart, paymentId } = JSON.parse(message.value.toString())

                // Business rule: reject the order if any item is out of stock.
                // This is an EXPECTED failure that triggers a compensating
                // transaction upstream — distinct from the poison-message
                // handling in the catch block below.
                // const unavailable = Array.isArray(cart)
                //     ? cart.filter((item) => outOfStockIds.has(String(item.id)))
                //     : []

                const unavailable = []


                if (unavailable.length > 0) {
                    const reason = `out of stock: ${unavailable.map((item) => item.name).join(", ")}`
                    logger.warn({ userId, paymentId, reason }, "Order failed — emitting compensation")

                    // Reverse event: payment-service consumes this to refund T1.
                    await producer.send({
                        topic: "order-failed",
                        // key = userId → all events for this user share a partition
                        // and stay ordered. Use an orderId/correlationId in real apps.
                        messages: [{ key: userId, value: JSON.stringify({ userId, cart, paymentId, reason }) }],
                    })
                    return
                }

                const orderId = "98765"
                logger.info({ userId, orderId }, "Order created")

                await producer.send({
                    topic: "order-successful",
                    messages: [{ key: userId, value: JSON.stringify({ userId, orderId }) }],
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
