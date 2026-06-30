/**
 * /config/kafka.js
 * Centralized Kafka connection manager.
 * Exports a single, robust Kafkajs instance to be used across all microservices.
 */

const { Kafka } = require('kafkajs');

// Initialize the core Kafka client
const kafka = new Kafka({
    // clientId acts as the "User Agent". Kafka logs use this to identify who is connecting.
    clientId: 'deep-tech-search-engine',
    
    // The connection string matching the KAFKA_ADVERTISED_LISTENERS in docker-compose
    brokers: ['localhost:9092'], 
    
    // The "Resilience" Configuration
    retry: {
        initialRetryTime: 100, // Wait 100ms before the first retry
        retries: 10 // If the Docker container restarts, don't crash the Node app immediately. Try 10 times.
    },
    
    // Disables noisy Kafka JS internal logs unless it's a fatal error
    logLevel: 1 
});

module.exports = kafka;