// TODO: Replace file with our rust raspi kafka producer
// labels: help wanted, enhancement
// milestone: raspi-integration

const { Kafka } = require("kafkajs");

// Create a Kafka client instance with the broker address
const kafka = new Kafka({
  clientId: "traffic-consumer",
  brokers: ["127.0.0.1:29092"], // Use 127.0.0.1 instead of localhost
  retry: {
    initialRetryTime: 3000, // Wait 3s before retrying
    retries: 10, // Retry 10 times
  },
});

// Helper functions for realistic data generation
const randomChoice = (options) =>
  options[Math.floor(Math.random() * options.length)];
const randomSubcount = (max) => Math.floor(Math.random() * (max + 1));

const createTrafficData = () => ({
  trafficData: {
    density: Math.floor(Math.random() * 100),
    travelTime: Math.floor(Math.random() * 60),
    vehicleNumber: Math.floor(Math.random() * 200),
    speed: Math.floor(Math.random() * 80),
    directionChange: randomChoice(["left", "right", "none"]),
    pedestrianCount: Math.floor(Math.random() * 50),
    bicycleCount: Math.floor(Math.random() * 20),
    heavyVehicleCount: Math.floor(Math.random() * 10),
    incidentDetected: Math.random() > 0.8,
    visibility: randomChoice(["good", "fair", "poor"]),
    weatherConditions: randomChoice(["sunny", "rain", "snow", "fog"]),
    roadCondition: randomChoice(["dry", "wet", "icy"]),
    congestionLevel: randomChoice(["low", "medium", "high"]),
    averageVehicleSize: randomChoice(["small", "medium", "large"]),
    vehicleTypeDistribution: {
      cars: Math.floor(Math.random() * 150),
      buses: Math.floor(Math.random() * 10),
      motorcycles: Math.floor(Math.random() * 30),
      trucks: Math.floor(Math.random() * 15),
    },
    trafficFlowDirection: randomChoice(["north-south", "east-west", "both"]),
    redLightViolations: Math.floor(Math.random() * 5),
    temperature: 15 + Math.floor(Math.random() * 25),
    humidity: Math.floor(Math.random() * 100),
    windSpeed: Math.floor(Math.random() * 40),
    airQualityIndex: Math.floor(Math.random() * 500),
    nearMissEvents: Math.floor(Math.random() * 5),
    accidentSeverity: randomChoice(["none", "minor", "major"]),
    roadworkDetected: Math.random() > 0.9,
    illegalParkingCases: Math.floor(Math.random() * 10),
  },
  intersectionData: {
    stoppedVehiclesCount: Math.floor(Math.random() * 50),
    averageWaitTime: Math.floor(Math.random() * 120),
    leftTurnCount: Math.floor(Math.random() * 30),
    rightTurnCount: Math.floor(Math.random() * 30),
    averageSpeedByDirection: {
      "north-south": Math.floor(Math.random() * 60),
      "east-west": Math.floor(Math.random() * 60),
    },
    laneOccupancy: Math.floor(Math.random() * 100),
    intersectionBlockingVehicles: Math.floor(Math.random() * 5),
    trafficLightComplianceRate: Math.floor(Math.random() * 100),
    pedestriansCrossing: Math.floor(Math.random() * 40),
    jaywalkingPedestrians: Math.floor(Math.random() * 10),
    cyclistsCrossing: Math.floor(Math.random() * 15),
    riskyBehaviorDetected: Math.random() > 0.7,
    queueLengthByLane: {
      lane1: Math.floor(Math.random() * 20),
      lane2: Math.floor(Math.random() * 20),
      lane3: Math.floor(Math.random() * 20),
    },
    intersectionCongestionLevel: randomChoice(["low", "medium", "high"]),
    intersectionCrossingTime: Math.floor(Math.random() * 120),
    trafficLightImpact: randomChoice(["low", "moderate", "high"]),
    nearMissIncidents: Math.floor(Math.random() * 5),
    collisionCount: Math.floor(Math.random() * 3),
    suddenBrakingEvents: Math.floor(Math.random() * 10),
    illegalParkingDetected: Math.random() > 0.8,
    wrongWayVehicles: Math.floor(Math.random() * 2),
    ambientLightLevel: Math.floor(Math.random() * 200),
    trafficLightStatus: randomChoice(["red", "yellow", "green"]),
    localWeatherConditions: randomChoice(["clear", "rain", "snow", "fog"]),
    fogOrSmokeDetected: Math.random() > 0.85,
  },
});

// Function to create the topic if it doesn't exist
const createTopic = async () => {
  const admin = kafka.admin(); // Create an admin client instance
  try {
    await admin.connect(); // Connect the admin client to Kafka
    const topics = await admin.listTopics(); // List all existing topics

    if (!topics.includes("traffic-data")) {
      // If the topic doesn't exist, create it with one partition and a replication factor of 1
      await admin.createTopics({
        topics: [
          {
            topic: "traffic-data",
            numPartitions: 1,
            replicationFactor: 1,
          },
        ],
      });
      console.log('Topic "traffic-data" created.');
    } else {
      console.log('Topic "traffic-data" already exists.');
    }
  } catch (error) {
    console.error("Error creating topic:", error);
  } finally {
    await admin.disconnect(); // Disconnect the admin client
  }
};

// Function to simulate and send traffic data using a Kafka producer
const produceTrafficData = async () => {
  const producer = kafka.producer(); // Create a producer instance
  try {
    await producer.connect(); // Connect the producer to Kafka
    console.log("Producer connected to Kafka");

    // Simulate mock traffic data
    const trafficData = createTrafficData();

    // Send the traffic data to the 'traffic-data' topic
    await producer.send({
      topic: "traffic-data",
      messages: [
        { value: JSON.stringify(trafficData) }, // Convert the data object to a JSON string
      ],
    });
    console.log("Traffic data sent:", trafficData);
  } catch (error) {
    console.error("Error in producer:", error);
  } finally {
    await producer.disconnect(); // Disconnect the producer
    console.log("Producer disconnected");
  }
};

// Main function to run the admin and producer steps sequentially
const run = async () => {
  await createTopic(); // Step 1: Create the topic (if needed)
  let isRunning = true;
  while (isRunning) {
    await produceTrafficData();
    await new Promise((resolve) => setTimeout(resolve, 5000)); // Send every 5 seconds
  } // Step 2: Send the traffic data
};

// Execute the run function
run();
