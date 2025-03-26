const { Kafka } = require("kafkajs");
const axios = require("axios"); 
const dotenv = require("dotenv");

dotenv.config();

// Configure the backend API URL
const BACKEND_API_URL = process.env.BACKEND_API_URL || "";

// Initialize Kafka client with a unique clientId and broker address
const kafka = new Kafka({
  clientId: "traffic-consumer",
  brokers: [process.env.KAFKA_BROKER || ""],
});

// Create a consumer instance within a consumer group
const consumer = kafka.consumer({ groupId: "traffic-group" });

// Helper function to send data to the backend
async function sendToBackend(endpoint, data) {
  try {
    const response = await axios.post(`${BACKEND_API_URL}${endpoint}`, data);
    console.log(`Data sent to ${endpoint} successfully, status: ${response.status}`);
    return response;
  } catch (error) {
    console.error(`Error sending data to ${endpoint}:`, error.message);
    if (error.response) {
      console.error(`Backend response: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
    }
    // In a production system, you might want to implement retry logic here
    throw error;
  }
}

const runConsumer = async () => {
  try {
    await consumer.connect();
    console.log("Consumer connected to Kafka");

    // Subscribe to all topics from the Rust producer
    await consumer.subscribe({ topic: "raw-vehicle-data", fromBeginning: true });
    await consumer.subscribe({ topic: "traffic-alerts", fromBeginning: true });
    await consumer.subscribe({ topic: "traffic-data", fromBeginning: true });
    await consumer.subscribe({ topic: "intersection-data", fromBeginning: true });
    await consumer.subscribe({ topic: "sensor-health", fromBeginning: true });
    
    console.log("Subscribed to all Rust producer topics");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const data = JSON.parse(message.value.toString());
          
          // Different endpoints for different topic types
          switch (topic) {
            case "raw-vehicle-data":
              console.log("Received vehicle data - sending to backend");
              await sendToBackend("/vehicles", data);
              break;
              
            case "traffic-alerts":
              console.log("Received traffic alert - sending to backend");
              await sendToBackend("/alerts", data);
              break;
              
            case "traffic-data":
              console.log("Received traffic data - sending to backend");
              await sendToBackend("/traffic", data);
              break;
              
            case "intersection-data":
              console.log("Received intersection data - sending to backend");
              await sendToBackend("/intersections", data);
              break;
              
            case "sensor-health":
              console.log("Received sensor health data - sending to backend");
              await sendToBackend("/sensor-health", data);
              break;
              
            default:
              console.log(`Received message from unknown topic: ${topic}`);
              await sendToBackend("/unknown", { topic, data });
          }

        } catch (error) {
          console.error(`Error processing message from topic ${topic}:`, error);
          console.error("Raw message:", message.value.toString());
        }
      },
    });
  } catch (error) {
    console.error("Error in consumer:", error);
  }
};

runConsumer();
