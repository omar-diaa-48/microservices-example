import cors from "cors"
import express from "express"
import { Kafka, Partitioners } from "kafkajs"
import { randomUUID } from "node:crypto"
import pino from "pino"
import { otelSdk } from "./tracing.js"

const logger = pino({ name: "payment-service" })

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9094,localhost:9095,localhost:9096").split(",")
const clientOrigin = process.env.CLIENT_ORIGIN ?? "http://localhost:3000"
const port = Number(process.env.PORT ?? 8000)

const app = express()

app.use(cors({
    origin: clientOrigin,
}))

app.use(express.json())

const kafka = new Kafka({
    clientId: "payment-service",
    brokers,
})

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
})

// Consumer for the compensation path. When a later saga step fails,
// order-service emits `order-failed`; this service compensates T1 (the charge)
// by refunding it. This reverse flow is what makes the event chain a real saga.
const consumer = kafka.consumer({
    groupId: "payment-service",
})

const cartTotal = (cart) =>
    Array.isArray(cart) ? cart.reduce((acc, item) => acc + item.price, 0) : 0

const connectToKafka = async () => {
    await producer.connect()
    await consumer.connect()
    await consumer.subscribe({
        topic: "order-failed",
    })

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const { userId, paymentId, cart, reason } = JSON.parse(message.value.toString())

                // Compensating transaction: reverse the charge from T1.
                // In production this MUST be idempotent — the same `order-failed`
                // event can be redelivered, and we must never refund twice.
                const amount = cartTotal(cart)
                logger.info({ userId, paymentId, amount, reason }, "Refund issued (compensating payment)")

                await producer.send({
                    topic: "payment-refunded",
                    messages: [{ value: JSON.stringify({ userId, paymentId, amount, reason }) }],
                })
            } catch (error) {
                // Swallow bad payloads so one poison message can't crash the consumer.
                // TODO: route failures to a dead-letter topic instead of dropping them.
                logger.error({ error }, "Failed to process order-failed message")
            }
        },
    })

    logger.info("Producer connected; refund consumer running")
}

app.post("/payment-service", async (req, res, next) => {
    try {
        const { cart } = req.body
        const userId = "12345"
        const paymentId = randomUUID()

        // Payment logic here — T1 of the saga: charge the customer.

        logger.info({ userId, paymentId }, "Payment successful")

        await producer.send({
            topic: "payment-successful",
            messages: [{ value: JSON.stringify({ cart, userId, paymentId }) }],
        })

        return res.status(200).send("Payment successful")
    } catch (error) {
        return next(error)
    }
})

app.use((err, req, res, next) => {
    logger.error({ err }, "Request error")
    res.status(err.status || 500).send(err.message)
})

const server = app.listen(port, async () => {
    logger.info({ port }, "Payment service is running")
    try {
        await connectToKafka()
    } catch (error) {
        logger.error({ error }, "Failed to connect Kafka producer")
    }
})

const shutdown = async (signal) => {
    logger.info({ signal }, "Shutting down payment-service")
    server.close()
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
