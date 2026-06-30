/**
 * /workers/indexer/database.js
 * The Universal Database Connector.
 * Maps and bulk-inserts multi-platform API payloads into Elasticsearch.
 */

const { Client } = require('@elastic/elasticsearch');
const crypto = require('crypto');

// Initialize the Elasticsearch client
const esClient = new Client({
    node: 'http://localhost:9200',
    tls: { rejectUnauthorized: false } 
});

// We update the index name to reflect the new universe-scale data
const INDEX_NAME = 'universal_issues';

/**
 * Verifies connection and enforces the Universal Schema.
 */
async function connectDatabase() {
    try {
        await esClient.ping();
        console.log('[+] Connected to Elasticsearch Engine.');

        const indexExists = await esClient.indices.exists({ index: INDEX_NAME });
        
        if (!indexExists) {
            // Create the index with our new structured properties
            await esClient.indices.create({
                index: INDEX_NAME,
                mappings: {
                    properties: {
                        source: { type: 'keyword' },       // Exact match only (e.g., "github", "reddit")
                        title: { type: 'text', analyzer: 'english' }, // NLP optimized search
                        body: { type: 'text', analyzer: 'english' },  // NLP optimized search
                        url: { type: 'keyword' },          // Exact match only, prevents tokenizing URLs
                        crawled_at: { type: 'date' }
                    }
                }
            });
            console.log(`[SUCCESS] Created Universal Schema index: ${INDEX_NAME}`);
        }
    } catch (error) {
        console.error('[-] Elasticsearch Connection Failed:', error.message);
        process.exit(1); // Stop the worker from booting if the database is dead
    }
}

/**
 * Performs a high-throughput Bulk Upsert into Elasticsearch.
 * @param {Array<{source: string, title: string, body: string, url: string}>} documents 
 */
async function bulkInsert(documents) {
    if (documents.length === 0) return;

    // Map the incoming Kafka API payloads to the exact Elasticsearch Bulk format
    const operations = documents.flatMap(doc => {
        // Idempotency: Hash the URL to create a globally unique, deterministic ID.
        // If GitHub gives us the same issue tomorrow, this ID ensures we overwrite the old one instead of duplicating it.
        const docId = crypto.createHash('sha256').update(doc.url).digest('hex');

        return [
            // Action Object: 'index' means Insert or Overwrite
            { index: { _index: INDEX_NAME, _id: docId } },
            // Data Object: The mapped Universal Schema payload
            { 
                source: doc.source,
                title: doc.title || 'Untitled', // Fallback in case an API sends a null title
                body: doc.body || '',           // Fallback in case an issue has no description
                url: doc.url,
                crawled_at: new Date().toISOString() 
            }
        ];
    });

    try {
        const bulkResponse = await esClient.bulk({ refresh: true, operations });

        // If Elasticsearch rejects any documents (e.g., wrong data type), we throw an error.
        // This will be caught by the Indexer worker, which will forward the batch to the Dead Letter Queue.
        if (bulkResponse.errors) {
            throw new Error('Elasticsearch rejected one or more documents due to schema conflicts.');
        } else {
            console.log(`[DATABASE] Safely inserted ${documents.length} universal issues.`);
        }
    } catch (error) {
        // Throwing the error back up to Kafka's eachBatch loop
        throw new Error(`Database Write Failed: ${error.message}`);
    }
}

module.exports = {
    connectDatabase,
    bulkInsert
};