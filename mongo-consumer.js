const { MongoClient } = require("mongodb");
const { Kafka } = require("kafkajs");
const dotenv = require("dotenv");

dotenv.config();

const MONGO_URI = process.env.MONGO_URI || "";
const DB_NAME = process.env.DB_NAME || "";


const TOPIC_TO_COLLECTION = {
  "raw-vehicle-data": "vehicle_records",
  "traffic-alerts": "alerts",
  "traffic-data": "traffic_metrics",
  "intersection-data": "intersections",
  "sensor-health": "sensor_health"
};

async function storeInMongo(data, topic) {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    
    // Select the appropriate collection based on the topic
    const collectionName = TOPIC_TO_COLLECTION[topic] || "unknown_data";
    const collection = db.collection(collectionName);

    // Prepare the document with common fields
    const document = {
      ...data,
      received_at: new Date(), // Add server receipt timestamp
    };

    // Add location data for geo queries if available
    if (topic === "traffic-data" && data.location_x !== undefined && data.location_y !== undefined) {
      // Use the actual coordinates from the message
      document.location = {
        type: "Point",
        coordinates: [data.location_x, data.location_y],
      };
    }

    // If this is the first document in the collection, create appropriate indices
    const count = await collection.countDocuments({}, { limit: 1 });
    if (count === 0) {
      await createIndices(collection, topic);
    }

    await collection.insertOne(document);
    console.log(`Stored ${topic} data in MongoDB collection: ${collectionName}`);
    
  } catch (error) {
    console.error(`MongoDB storage failed for topic ${topic}:`, error);
  } finally {
    await client.close();
  }
}

// Create  indices 
async function createIndices(collection, topic) {
  try {
    await collection.createIndex({ timestamp: 1 });
    
    
    switch (topic) {
      case "raw-vehicle-data":
        await collection.createIndex({ sensor_id: 1 });
        await collection.createIndex({ vehicle_class: 1 });
        await collection.createIndex({ speed_kmh: 1 });
        break;
        
      case "traffic-alerts":
        await collection.createIndex({ type: 1 });
        await collection.createIndex({ sensor_id: 1 });
        break;
        
      case "traffic-data":
        await collection.createIndex({ sensor_id: 1 });
        await collection.createIndex({ location_id: 1 });
        await collection.createIndex({ "location": "2dsphere" });
        await collection.createIndex({ congestion_level: 1 });
        await collection.createIndex({ incident_detected: 1 });
        break;
        
      case "intersection-data":
        await collection.createIndex({ sensor_id: 1 });
        await collection.createIndex({ intersection_id: 1 });
        await collection.createIndex({ intersection_congestion_level: 1 });
        break;
        
      case "sensor-health":
        await collection.createIndex({ sensor_id: 1 });
        await collection.createIndex({ hw_fault: 1 });
        await collection.createIndex({ low_voltage: 1 });
        break;
    }
    
    console.log(`Created indices for ${topic} collection`);
  } catch (error) {
    console.error(`Error creating indices for ${topic}:`, error);
  }
}
const kafka = new Kafka({ 
  brokers: [process.env.KAFKA_BROKER || ""],
  clientId: "mongo-storage-consumer"
});
const consumer = kafka.consumer({ groupId: "mongo-storage-group" });

async function runMongoConsumer() {
  try {
    await consumer.connect();
    console.log("MongoDB consumer connected to Kafka");
    
    await consumer.subscribe({ topic: "raw-vehicle-data", fromBeginning: true });
    await consumer.subscribe({ topic: "traffic-alerts", fromBeginning: true });
    await consumer.subscribe({ topic: "traffic-data", fromBeginning: true });
    await consumer.subscribe({ topic: "intersection-data", fromBeginning: true });
    await consumer.subscribe({ topic: "sensor-health", fromBeginning: true });
    
    console.log("Subscribed to all Rust producer topics for MongoDB storage");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const data = JSON.parse(message.value.toString());
          await storeInMongo(data, topic);
        } catch (error) {
          console.error(`Error processing ${topic} message for MongoDB:`, error);
          console.error("Raw message:", message.value.toString());
        }
      },
    });
  } catch (error) {
    console.error("Error in MongoDB consumer:", error);
  }
}

console.log("Starting MongoDB storage consumer for traffic data...");
runMongoConsumer();

