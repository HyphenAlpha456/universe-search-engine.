/**
 * /workers/indexer/index.js
 * The Database Ingestion Daemon.
 * Consumes raw text payloads and synchronizes them into Elasticsearch.
 */

const kafka = require('../../config/kafka');
const { connectDatabase, bulkInsert } = require('./database');

async function startIndexer() {
    // 1. Initialize the Consumer with a strict Group ID
    // Kafka uses this ID to track which worker owns which partition
    const consumer = kafka.consumer({ groupId: 'indexer-group' });

    try {
        console.log('[+] Booting Indexer Node...');
        
        // 2. Boot dependencies before connecting to the message broker
        await connectDatabase();
        await consumer.connect();

        // 3. Subscribe to the firehose topic
        // fromBeginning: false ensures we only process new data, ignoring old logs on restart
        await consumer.subscribe({ topic: 'scraped_payloads', fromBeginning: false });

        console.log('[+] Indexer Worker running. Waiting for payloads...');

        // 4. The Infinite Polling Loop
        await consumer.run({
            // CRITICAL: Disable auto-commit. We only commit after the DB saves the data.
            autoCommit: false,
            
            // eachBatch allows us to process 100+ messages in a single array
            eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
                
                console.log(`[PULL] Received ${batch.messages.length} documents from Partition ${batch.partition}`);
                
                const documents = [];

                // 5. Unpack the Kafka byte payloads back into JSON
                for (let message of batch.messages) {
                    const payload = JSON.parse(message.value.toString());
                    documents.push(payload);
                    
                    // Mark the message as processed in local RAM
                    resolveOffset(message.offset);
                }

                // 6. The Backpressure Check
                // If Elasticsearch fails (e.g., returns 429 Too Many Requests), bulkInsert will throw an error.
                // This breaks the loop BEFORE commitOffsetsIfNecessary() is called. 
                // The Node crashes, Kafka sees the failure, and NO data is lost.
                await bulkInsert(documents);

                // Send a heartbeat to Kafka to prove the worker is still alive during heavy DB writes
                await heartbeat();

                // 7. The Atomic Save
                // Write the final offset to the __consumer_offsets hidden topic
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