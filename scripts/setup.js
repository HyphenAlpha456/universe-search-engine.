/**
 * /scripts/setup.js
 * Infrastructure-as-Code execution.
 * Formats the Kafka partitions for API Firehose ingestion and Error Handling.
 */

const kafka = require('../config/kafka');

async function initializeInfrastructure() {
    const admin = kafka.admin();

    try {
        console.log('[+] Booting Admin API and connecting to KRaft Broker...');
        await admin.connect();
        
        const topicsToCreate = [
            {
                topic: 'universal_payloads',
                numPartitions: 10,     
                replicationFactor: 1   
            },
            {
                topic: 'failed_payloads', 
                numPartitions: 2,         
                replicationFactor: 1
            }
        ];

        console.log('[+] Executing physical hard-drive formatting...');
        
        const success = await admin.createTopics({
            topics: topicsToCreate,
            waitForLeaders: true, 
        });

        if (success) {
            console.log('[SUCCESS] Universal Firehose and DLQ partitions successfully allocated.');
        } else {
            console.log('[INFO] Topics already exist. Storage architecture is secure.');
        }

    } catch (error) {
        console.error('[-] Fatal Setup Error:', error.message);
    } finally {
        await admin.disconnect();
        console.log('[+] Admin network connection closed.');
    }
}

initializeInfrastructure();