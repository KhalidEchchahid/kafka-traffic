const { Kafka } = require("kafkajs")
const axios = require("axios")
const dotenv = require("dotenv")

dotenv.config()

// Check for required environment variables
const KAFKA_BROKER = process.env.KAFKA_BROKER
if (!KAFKA_BROKER) {
  console.error("ERROR: KAFKA_BROKER environment variable is required")
  process.exit(1)
}

const BACKEND_API_URL = process.env.BACKEND_API_URL || "http://localhost:3001/api/receive"
console.log(`Using backend API URL: ${BACKEND_API_URL}`)
console.log(`Using Kafka broker: ${KAFKA_BROKER}`)

// Create Kafka client
const kafka = new Kafka({
  clientId: "traffic-consumer",
  brokers: KAFKA_BROKER.split(","), // Support multiple brokers
})

const consumer = kafka.consumer({ groupId: "traffic-group" })

// Helper function to send data to the backend
async function sendToBackend(endpoint, data) {
  try {
    // Ensure the URL is constructed correctly with a slash between receive and endpoint
    let url = BACKEND_API_URL

    // Make sure the base URL ends with a slash
    if (!url.endsWith("/")) {
      url += "/"
    }

    // Make sure the endpoint doesn't start with a slash
    if (endpoint.startsWith("/")) {
      endpoint = endpoint.substring(1)
    }

    // Construct the final URL
    const finalUrl = `${url}${endpoint}`
    console.log(`Sending data to: ${finalUrl}`)

    const response = await axios.post(finalUrl, data)
    console.log(`Data sent to ${endpoint} successfully, status: ${response.status}`)
    return response
  } catch (error) {
    console.error(`Error sending data to ${endpoint}:`, error.message)
    if (error.response) {
      console.error(`Backend response: ${error.response.status} - ${JSON.stringify(error.response.data)}`)
    } else if (error.request) {
      console.error("No response received from backend. Is it running?")
    }
    throw error
  }
}

const runConsumer = async () => {
  try {
    await consumer.connect()
    console.log("Consumer connected to Kafka")

    // Subscribe to all topics from the Rust producer
    const topics = ["raw-vehicle-data", "traffic-alerts", "traffic-data", "intersection-data", "sensor-health"]

    for (const topic of topics) {
      await consumer.subscribe({
        topic,
        fromBeginning: true,
      })
      console.log(`Subscribed to topic: ${topic}`)
    }

    console.log("Subscribed to all Rust producer topics")

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          console.log(`Received message from topic: ${topic}`)
          const data = JSON.parse(message.value.toString())

          // Map topics to the correct backend endpoints
          // Note: These should match your backend API endpoints
          const topicToEndpoint = {
            "raw-vehicle-data": "vehicle",
            "traffic-alerts": "alert",
            "traffic-data": "traffic",
            "intersection-data": "intersection",
            "sensor-health": "sensor",
          }

          const endpoint = topicToEndpoint[topic]
          if (endpoint) {
            console.log(`Sending ${topic} data to endpoint: ${endpoint}`)
            await sendToBackend(endpoint, data)
          } else {
            console.warn(`No endpoint mapping for topic: ${topic}`)
          }
        } catch (error) {
          console.error(`Error processing message from topic ${topic}:`, error)
          console.error("Raw message:", message.value.toString())
        }
      },
    })
  } catch (error) {
    console.error("Error in consumer:", error)
    // Try to disconnect gracefully
    try {
      await consumer.disconnect()
    } catch (e) {
      console.error("Error disconnecting consumer:", e)
    }
    process.exit(1)
  }
}

// Handle graceful shutdown
process.on("SIGINT", async () => {
  console.log("Received SIGINT. Shutting down...")
  try {
    await consumer.disconnect()
    console.log("Consumer disconnected")
  } catch (e) {
    console.error("Error during shutdown:", e)
  }
  process.exit(0)
})

process.on("SIGTERM", async () => {
  console.log("Received SIGTERM. Shutting down...")
  try {
    await consumer.disconnect()
    console.log("Consumer disconnected")
  } catch (e) {
    console.error("Error during shutdown:", e)
  }
  process.exit(0)
})

// Start the consumer
runConsumer().catch((error) => {
  console.error("Fatal error:", error)
  process.exit(1)
})

