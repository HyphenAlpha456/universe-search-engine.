/**
 * /workers/ingestion/stackoverflow.js
 * Production-Ready: Sliding-Window Stack Exchange API Firehose
 */

const axios = require('axios');
const kafka = require('../../config/kafka');

// Stack Exchange requires a 'key' for higher daily quotas (10,000 req/day).
// Without it, you are limited to 300 req/day.
const STACKAPPS_KEY = process.env.STACKAPPS_KEY || ''; 
const TOPIC = 'universal_payloads';

// Utility: Block event loop for rate limiting
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

class StackOverflowIngestor {
    constructor() {
        this.producer = kafka.producer();
        
        // Axios configured for Stack Exchange's specific gzip compression and URL structure
        this.api = axios.create({
            baseURL: 'https://api.stackexchange.com/2.3',
            // Stack Exchange APIs require compression
            headers: { 'Accept-Encoding': 'gzip, deflate' } 
        });
    }

    async init() {
        await this.producer.connect();
        console.log('[+] Connected to Kafka Firehose. Booting StackOverflow Node...');
    }

    /**
     * Executes the network request and handles Stack Exchange's specific quota/backoff rules.
     */
    async safeFetch(endpoint, params) {
        while (true) {
            try {
                // Inject the API key into the params if it exists
                if (STACKAPPS_KEY) params.key = STACKAPPS_KEY;
                
                const response = await this.api.get(endpoint, { params });
                const data = response.data;

                // 1. The Backoff Trap: Stack Exchange dynamically tells you to back off.
                // If you ignore this field, you will be permanently banned.
                if (data.backoff) {
                    console.warn(`\n[!] API instructed backoff. Halting for ${data.backoff} seconds...`);
                    await sleep(data.backoff * 1000);
                } else {
                    // Standard throttle to play nice with their servers (30 req / sec max allowed)
                    await sleep(500); 
                }

                // 2. The Quota Monitor
                if (data.quota_remaining < 50) {
                    console.warn(`[WARNING] Daily Quota critically low: ${data.quota_remaining} left.`);
                }

                return data;

            } catch (error) {
                if (error.response?.status === 400 || error.response?.status === 422) {
                    console.error(`[-] API Error (Throttle/Format): ${error.response.data.error_message}`);
                    await sleep(60000); // Heavy backoff on strict errors
                } else {
                    console.error(`[-] Network Error: ${error.message}. Retrying in 5s...`);
                    await sleep(5000);
                }
            }
        }
    }

    /**
     * Extracts all questions within a strict Unix timestamp window.
     */
    async extractTimeWindow(fromUnix, toUnix) {
        console.log(`\n[EXTRACT] Window: ${new Date(fromUnix * 1000).toISOString()} -> ${new Date(toUnix * 1000).toISOString()}`);
        let page = 1;
        let hasMore = true;

        while (hasMore) {
            const data = await this.safeFetch('/questions', {
                site: 'stackoverflow',
                page: page,
                pagesize: 100,           // Maximize batch size
                fromdate: fromUnix,
                todate: toUnix,
                order: 'desc',
                sort: 'creation',
                filter: 'withbody'       // CRITICAL: Tells the API to include the issue text, not just metadata
            });

            if (!data.items || data.items.length === 0) break;

            // Universal Schema Mapper
            const mappedPayloads = data.items.map(q => ({
                value: JSON.stringify({
                    source: 'stackoverflow',
                    title: q.title || 'Untitled', // Title includes HTML entities, Elasticsearch handles indexing it
                    body: q.body_markdown || q.body || '', 
                    url: q.link
                })
            }));

            // Buffer dump to Kafka
            await this.producer.send({
                topic: TOPIC,
                messages: mappedPayloads
            });

            console.log(`  -> [PUSH] Buffered ${mappedPayloads.length} issues (Page ${page}). Quota Left: ${data.quota_remaining}`);
            
            hasMore = data.has_more;
            page++;
        }
    }

    /**
     * The Master Loop: Slides a 24-hour window backwards through time.
     */
    async backfillUniverse(startUnixTarget) {
        // Start at current time (in Unix seconds)
        let toDate = Math.floor(Date.now() / 1000); 
        const ONE_DAY_SECONDS = 86400;

        while (toDate > startUnixTarget) {
            let fromDate = toDate - ONE_DAY_SECONDS;
            
            // Extract the 24-hour block
            await this.extractTimeWindow(fromDate, toDate);
            
            // Slide the window back one day
            toDate = fromDate; 
        }
    }

    async shutdown() {
        console.log('\n[SHUTDOWN] Closing Kafka connection...');
        await this.producer.disconnect();
        process.exit(0);
    }
}

// ----------------------------------------------------------------------------
// Execution Entry Point
// ----------------------------------------------------------------------------

// Target: January 1, 2020 (Unix Epoch in seconds)
const START_TARGET_UNIX = 1577836800; 

const worker = new StackOverflowIngestor();

process.on('SIGINT', () => worker.shutdown());

worker.init().then(() => {
    console.log(`[+] Commencing Stack Overflow Historical Backfill...`);
    worker.backfillUniverse(START_TARGET_UNIX)
        .then(() => {
            console.log('[SUCCESS] Universe segment fully ingested.');
            worker.shutdown();
        })
        .catch(err => console.error('[-] Fatal Worker Exception:', err));
});