const express = require('express');
const { Client } = require('@elastic/elasticsearch');
const router = express.Router();

const client = new Client({ node: 'http://localhost:9200' });

router.get('/', async (req, res) => {
    try {
        const { q, source } = req.query;

        if (!q) return res.json({ hits: [] });

        const filter = [];
        if (source && source !== 'all') {
            filter.push({ term: { 'source.keyword': source } });
        }

        const searchBody = {
            query: {
                bool: {
                    must: filter,
                    should: [
                        { match_phrase: { title: { query: q, boost: 10 } } },
                        { match_phrase: { body: { query: q, boost: 5 } } },
                        { multi_match: { query: q, fields: ['title^3', 'body'], fuzziness: 'AUTO' } }
                    ],
                    minimum_should_match: 1
                }
            },
            size: 50,
            sort: [{ _score: { order: 'desc' } }]
        };

        const result = await client.search({
            index: 'universal_issues',
            body: searchBody
        });

        const hits = result.hits.hits.map(hit => ({
            id: hit._id,
            ...hit._source
        }));

        res.json({ hits });
    } catch (error) {
        console.error('[-] Search Error:', error);
        res.status(500).json({ error: 'Search failed' });
    }
});

module.exports = router;