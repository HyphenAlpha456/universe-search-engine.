/**
 * /workers/indexer/index.js
 * Production-Ready: Kafka-to-Elasticsearch Universal Indexer (Team B)
 */

const kafka = require('../../config/kafka');
const { connectDatabase, bulkInsert } = require('./database');

const MAIN_TOPIC = 'universal_payloads';
const DLQ_TOPIC = 'failed_payloads';
const GROUP_ID = 'universe-indexer-group';

class UniversalIndexer {
    constructor() {
        this.consumer = kafka.consumer({ groupId: GROUP_ID });
        this.dlqProducer = kafka.producer(); // Needed to send errors to the DLQ
    }

    async init() {
        // 1. Ensure the database is awake and the schema is formatted
        await connectDatabase();

        // 2. Connect Kafka clients
        await this.dlqProducer.connect();
        await this.consumer.connect();
        
        console.log('[+] Indexer connected to Kafka cluster.');

        // Subscribe to the firehose from the beginning of time
        await this.consumer.subscribe({ topic: MAIN_TOPIC, fromBeginning: true });
    }

    /**
     * Routes corrupted data to the Dead Letter Queue to prevent pipeline blocking.
     */
    async routeToDLQ(batch, error) {
        console.warn(`[DLQ] Routing batch of ${batch.messages.length} messages to Dead Letter Queue.`);
        
        const dlqMessages = batch.messages.map(msg => ({
            key: msg.key || 'unknown',
            value: JSON.stringify({
                original_payload: msg.value.toString(),
                error_reason: error.message,
                failed_at: new Date().toISOString()
            })
        }));

        await this.dlqProducer.send({
            topic: DLQ_TOPIC,
            messages: dlqMessages
        });
    }

    async startProcessing() {
        console.log(`[+] Listening for data on [${MAIN_TOPIC}]...`);

        // Use eachBatch for high-throughput, bulk-insert performance
        await this.consumer.run({
            partitionsConsumedConcurrently: 3, // Process up to 3 partitions simultaneously per worker
            eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
                
                if (!isRunning() || isStale()) return;

                const parsedDocuments = [];

                // 1. Parse the Kafka binary buffers into JSON
                for (const message of batch.messages) {
                    try {
                        const doc = JSON.parse(message.value.toString());
                        parsedDocuments.push(doc);
                    } catch (parseError) {
                        // Extreme edge case: The ingestor sent malformed JSON
                        console.error(`[-] Critical Parse Error: ${parseError.message}`);
                    }
                }

                if (parsedDocuments.length === 0) return;

                // 2. The Database Write Attempt
                try {
                    await bulkInsert(parsedDocuments);
                    
                    // Tell Kafka we successfully saved these documents
                    resolveOffset(batch.messages[batch.messages.length - 1].offset);
                    
                    // Ping the Kafka broker so it knows this worker is still alive
                    await heartbeat();
                    
                } catch (dbError) {
                    // 3. The DLQ Trigger (Poison Pill caught)
                    console.error(`[-] Database Write Failed: ${dbError.message}`);
                    
                    try {
                        await this.routeToDLQ(batch, dbError);
                        
                        // CRITICAL: We must resolve the offset even if it failed.
                        // We saved it to the DLQ, so it is safe to move on. 
                        // If we don't resolve, Kafka will send us this exact broken batch again instantly.
                        resolveOffset(batch.messages[batch.messages.length - 1].offset);
                        await heartbeat();
                        
                    } catch (dlqError) {
                        // If the DLQ itself is broken, the cluster is critically failing. Let it crash.
                        console.error(`[FATAL] Dead Letter Queue routing failed: ${dlqError.message}`);
                        throw dlqError; 
                    }
                }
            },
        });
    }

    async shutdown() {
        console.log('\n[SHUTDOWN] Gracefully committing offsets and closing Indexer...');
        try {
            await this.consumer.disconnect();
            await this.dlqProducer.disconnect();
            console.log('[SUCCESS] Indexer closed safely.');
        } catch (e) {
            console.error('[-] Error during shutdown:', e);
        }
        process.exit(0);
    }
}

// ----------------------------------------------------------------------------
// Execution Entry Point
// ----------------------------------------------------------------------------
const worker = new UniversalIndexer();

// Catch termination signals to prevent data corruption during container restarts
process.on('SIGINT', () => worker.shutdown());
process.on('SIGTERM', () => worker.shutdown());

worker.init()
    .then(() => worker.startProcessing())
    .catch(err => {
        console.error('[-] Fatal Indexer Boot Error:', err);
        process.exit(1);
    });