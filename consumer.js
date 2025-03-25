const { Kafka } = require("kafkajs");

// Initialize Kafka client with a unique clientId and broker address
const kafka = new Kafka({
  clientId: "traffic-consumer",
  brokers: ["localhost:29092"],
});

// Create a consumer instance within a consumer group
const consumer = kafka.consumer({ groupId: "traffic-group" });

const runConsumer = async () => {
  try {
    await consumer.connect();
    console.log("Consumer connected to Kafka");

    // TODO: Implement all our kafka topics
    //  We need to setup further topics, like traffic-alert sensor-health etc, see Rust
    //  repo for more info.
    //  labels: help wanted, enhancement
    //  milestone: raspi-integration
    await consumer.subscribe({ topic: "traffic-data", fromBeginning: true });
    console.log("Subscribed to topic: traffic-data");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const fullData = JSON.parse(message.value.toString());

          // Extract key metrics
          const { trafficData, intersectionData } = fullData;
          const summary = {
            timestamp: new Date().toISOString(),
            density: trafficData.density,
            congestion: trafficData.congestionLevel,
            incidents: trafficData.incidentDetected ? 1 : 0,
            avgSpeed: trafficData.speed,
            queueLength: Object.values(
              intersectionData.queueLengthByLane,
            ).reduce((a, b) => a + b, 0),
          };

          console.log("Received traffic update:");
          console.log("Summary:", summary);
          console.log("Full data:", fullData);
        } catch (error) {
          console.error("Error processing message:", error);
        }
      },
    });
  } catch (error) {
    console.error("Error in consumer:", error);
  }
};

runConsumer();
