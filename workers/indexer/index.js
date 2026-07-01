/**
 * /workers/indexer/index.js
 * The Database Ingestion Daemon.
 * Consumes raw text payloads and synchronizes them into Elasticsearch.
 */

const kafka = require('../../config/kafka');
const { connectDatabase, bulkInsert } = require('./database');

async function startIndexer() {
    // [FIX 1]: Changed groupId to v3 to wipe Kafka's memory of the old bookmarks.
    // This ensures fromBeginning: true actually triggers a full historical backfill.
    const consumer = kafka.consumer({ groupId: 'indexer-group-v3' });

    try {
        console.log('[+] Booting Indexer Node...');
        
        // Boot dependencies before connecting to the message broker
        await connectDatabase();
        await consumer.connect();

        // [FIX 2 & 3]: Subscribed to the exact topic github.js is writing to,
        // and forced it to act like a podcast (read from the very start).
        await consumer.subscribe({ 
            topic: 'universal_payloads', 
            fromBeginning: true 
        });

        console.log('[+] Indexer Worker running. Waiting for payloads...');

        // The Infinite Polling Loop
        await consumer.run({
            // CRITICAL: Disable auto-commit. We only commit after the DB saves the data.
            autoCommit: false,
            
            // eachBatch allows us to process messages in massive chunks
            eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
                
                console.log(`[PULL] Received ${batch.messages.length} documents from Partition ${batch.partition}`);
                
                const documents = [];

                // Unpack the Kafka byte payloads back into JSON
                for (let message of batch.messages) {
                    const payload = JSON.parse(message.value.toString());
                    documents.push(payload);
                    
                    // Mark the message as processed in local RAM
                    resolveOffset(message.offset);
                }

                // The Backpressure Check - Push to Elasticsearch
                await bulkInsert(documents);

                // Send a heartbeat to Kafka to prove the worker is still alive during DB writes
                await heartbeat();

                // The Atomic Save - write final offset to Kafka's hidden topic
                await commitOffsetsIfNecessary();
                
                console.log(`[COMMIT] Safely indexed batch. Listening for more...`);
            }
        });

    } catch (error) {
        console.error('[-] Fatal Indexer Error:', error.message);
        process.exit(1);
    }
}

// Intercept graceful shutdown signals (Ctrl+C or Docker Stop)
process.on('SIGINT', async () => {
    console.log('\n[SHUTDOWN] Gracefully closing Indexer...');
    process.exit(0);
});

startIndexer();