import { otelSdk } from "./tracing.js"
import cors from "cors"
import express from "express"
import { Kafka, Partitioners } from "kafkajs"
import pino from "pino"

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

const connectToKafka = async () => {
    await producer.connect()
    logger.info("Producer connected")
}

app.post("/payment-service", async (req, res, next) => {
    try {
        const { cart } = req.body
        const userId = "12345"

        // Payment logic here

        logger.info({ userId }, "Payment successful")

        await producer.send({
            topic: "payment-successful",
            messages: [{ value: JSON.stringify({ cart, userId }) }],
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
