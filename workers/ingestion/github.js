/**
 * /workers/ingestion/github.js
 * Production-Ready: Recursive Time-Slicing GitHub API Firehose
 */

const axios = require('axios');
const kafka = require('../../config/kafka');

const GITHUB_TOKEN = process.env.GITHUB_TOKEN || '';
const TOPIC = 'universal_payloads';

// Utility: Block event loop for rate limiting
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Utility: Calculate midpoint between two ISO 8601 timestamps
const getMidpoint = (startIso, endIso) => {
    const start = new Date(startIso).getTime();
    const end = new Date(endIso).getTime();
    return new Date(Math.floor((start + end) / 2)).toISOString();
};

class GitHubIngestor {
    constructor() {
        this.producer = kafka.producer();
        // Axios instance configured for GitHub's REST API
        this.api = axios.create({
            baseURL: 'https://api.github.com',
            headers: {
                'Accept': 'application/vnd.github.v3+json',
                ...(GITHUB_TOKEN && { 'Authorization': `Bearer ${GITHUB_TOKEN}` })
            }
        });
    }

    async init() {
        await this.producer.connect();
        console.log('[+] Connected to Kafka Firehose.');
    }

    /**
     * Executes a network request while aggressively defending against GitHub's Rate Limits.
     */
    async safeFetch(url, params) {
        while (true) {
            try {
                const response = await this.api.get(url, { params });
                
                // Rate Limit Defender
                const remaining = parseInt(response.headers['x-ratelimit-remaining'] || '10');
                if (remaining < 5) {
                    const resetUnix = parseInt(response.headers['x-ratelimit-reset'] || '0');
                    const waitMs = Math.max((resetUnix * 1000) - Date.now() + 5000, 10000);
                    console.warn(`\n[!] Rate Limit Critical. Sleeping for ${waitMs / 1000}s...`);
                    await sleep(waitMs);
                } else {
                    await sleep(1500); // Standard throttle
                }
                
                return response.data;
            } catch (error) {
                if (error.response?.status === 429 || error.response?.status === 403) {
                    console.error('[-] HTTP 429/403: Abuse protection triggered. Backing off 60s...');
                    await sleep(60000);
                } else {
                    console.error(`[-] API Fetch Error: ${error.message}. Retrying in 5s...`);
                    await sleep(5000);
                }
            }
        }
    }

    /**
     * Queries only the metadata (total_count) to evaluate the time segment.
     */
    async getSegmentCount(start, end) {
        const query = `is:issue is:public created:${start}..${end}`;
        console.log(`[EVALUATE] Segment: ${start} -> ${end}`);
        const data = await this.safeFetch('/search/issues', { q: query, per_page: 1 });
        return data.total_count || 0;
    }

    /**
     * Extracts all data from a validated leaf node (<= 1000 results) and pushes to Kafka.
     */
    async extractLeafNode(start, end, totalCount) {
        console.log(`[EXTRACT] Leaf Node Found (${totalCount} issues). Downloading...`);
        const query = `is:issue is:public created:${start}..${end}`;
        const pages = Math.ceil(totalCount / 100);

        for (let page = 1; page <= pages; page++) {
            const data = await this.safeFetch('/search/issues', { q: query, per_page: 100, page: page });
            
            if (!data.items || data.items.length === 0) break;

            const mappedPayloads = data.items.map(issue => ({
                value: JSON.stringify({
                    source: 'github',
                    title: issue.title || 'Untitled',
                    body: issue.body || '',
                    url: issue.html_url
                })
            }));

            await this.producer.send({
                topic: TOPIC,
                messages: mappedPayloads
            });
            console.log(`  -> [PUSH] Buffered ${mappedPayloads.length} issues (Page ${page}/${pages})`);
        }
    }

    /**
     * The core Divide and Conquer algorithm.
     */
    async processTimeSlice(startIso, endIso) {
        const count = await this.getSegmentCount(startIso, endIso);

        if (count === 0) {
            console.log(`[PRUNE] 0 issues found. Branch dead.`);
            return;
        }

        if (count > 1000) {
            console.log(`[SPLIT] ${count} > 1000 limit. Bisecting segment...`);
            const midIso = getMidpoint(startIso, endIso);
            
            // Traverse Left (Older)
            await this.processTimeSlice(startIso, midIso);
            // Traverse Right (Newer)
            await this.processTimeSlice(midIso, endIso);
        } else {
            // Base Case: Count is valid. Extract.
            await this.extractLeafNode(startIso, endIso, count);
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
const startTarget = '2023-01-01T00:00:00Z'; // Modify to determine backfill range
const endTarget = new Date().toISOString();  // Up to current exact second

const worker = new GitHubIngestor();

process.on('SIGINT', () => worker.shutdown());

worker.init().then(() => {
    console.log(`[+] Commencing Historical Backfill: ${startTarget} to ${endTarget}`);
    worker.processTimeSlice(startTarget, endTarget)
        .then(() => {
            console.log('[SUCCESS] Universe segment fully ingested.');
            worker.shutdown();
        })
        .catch(err => console.error('[-] Fatal Worker Exception:', err));
});