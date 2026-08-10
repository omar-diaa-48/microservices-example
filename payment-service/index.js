import cors from "cors";
import express from "express";
import { Kafka, Partitioners } from "kafkajs";


const app = express()

app.use(cors({
    origin: "http://localhost:3000"
}))

app.use(express.json())

const kafka = new Kafka({
    clientId: "payment-service",
    brokers: ["localhost:9094"]
})

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner
})

const connectToKafka = async () => {
    try {
        await producer.connect()
        console.log("Producer connected!")
    } catch (error) {
        console.error("Error connection to kafka", error)
    }
}

app.post("/payment-service", async (req, res) => {
    const { cart } = req.body
    const userId = "12345"

    // Payment logic here

    console.log(`Payment successful for user ${userId}`)

    // KAFKA
    await producer.send({
        topic: 'payment-successful',
        messages: [
            {
                value: JSON.stringify({
                    cart,
                    userId
                })
            }
        ]
    })

    return res.status(200).send("Payment successful")
})

app.use((err, req, res, next) => {
    res.status(err.status || 500).send(err.message)
})

app.listen(8000, () => {
    connectToKafka()
    console.log("Payment service is running on port 8000")
})