const { MongoClient } = require("mongodb");
const { Kafka } = require("kafkajs");

// Connect to MongoDB
const MONGO_URI = "mongodb://admin:admin@localhost:27017";
const DB_NAME = "traffic_db";
const COLLECTION_NAME = "traffic_metrics";

async function storeInMongo(data) {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    // Add timestamp and create geospatial index if needed
    const document = {
      ...data,
      timestamp: new Date(),
      location: {
        // Example geospatial data (modify as needed)
        type: "Point",
        coordinates: [-73.856077, 40.848447],
      },
    };

    await collection.insertOne(document);
  } finally {
    await client.close();
  }
}

// Kafka consumer logic (similar to your existing consumer.js)
const kafka = new Kafka({ brokers: ["127.0.0.1:29092"] });
const consumer = kafka.consumer({ groupId: "mongo-group" });

async function runMongoConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: "traffic-data" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        await storeInMongo(data);
        console.log("Data stored in MongoDB");
      } catch (error) {
        console.error("MongoDB storage failed:", error);
      }
    },
  });
}

runMongoConsumer();

