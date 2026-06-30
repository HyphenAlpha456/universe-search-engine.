const express = require('express');
const { Client } = require('@elastic/elasticsearch');
const router = express.Router();

const client = new Client({ node: 'http://localhost:9200' });

router.get('/', async (req, res) => {
    try {
        const { q, source } = req.query;

        if (!q) {
            return res.json({ hits: [] });
        }

        const mustClauses = [
            {
                multi_match: {
                    query: q,
                    fields: ['title^3', 'body'],
                    fuzziness: 'AUTO'
                }
            }
        ];

        if (source && source !== 'all') {
            mustClauses.push({
                term: { 'source.keyword': source }
            });
        }

        const result = await client.search({
            index: 'universal_issues',
            body: {
                query: {
                    bool: {
                        must: mustClauses
                    }
                },
                size: 50,
                sort: [{ _score: 'desc' }]
            }
        });

        const hits = result.hits.hits.map(hit => ({
            id: hit._id,
            ...hit._source
        }));

        res.json({ hits });
    } catch (error) {
        res.status(500).json({ error: 'Search failed' });
    }
});

module.exports = router;