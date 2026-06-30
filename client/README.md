# Universe Search Engine 🌌

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)]()
[![Node Version](https://img.shields.io/badge/Node-v18%2B-green)]()

A high-throughput, distributed data platform engineered to backfill, stream, and index millions of developer issues from GitHub and Stack Overflow. This project is a demonstration of large-scale systems engineering, event-driven architecture, and scalable full-text search.

---

## 🏗️ System Architecture

The system is built as a decoupled, event-driven microservices ecosystem. It ensures fault tolerance and low latency by separating ingestion, buffering, and storage tiers.



## 🛠 Tech Stack

* **Ingestion:** Node.js, Axios, REST API
* **Message Broker:** Apache Kafka (Event streaming & buffering)
* **Search Engine:** Elasticsearch (Lucene-based full-text indexing)
* **Frontend:** React.js, Redux Toolkit, Vite
* **Infrastructure:** Docker, Docker Compose

## 💡 Key Engineering Challenges Solved

* **Rate-Limit Resilience:** Implemented a **Recursive Divide & Conquer algorithm** for GitHub to bisect time-windows and bypass the 1,000-result pagination wall, ensuring 100% data capture.
* **Backpressure Handling:** Utilized Kafka partitions and `eachBatch` processing to buffer high-velocity API data, protecting the database layer from traffic spikes.
* **Fault Tolerance:** Engineered a **Dead Letter Queue (DLQ)** to isolate malformed payloads, ensuring the main pipeline continues running without human intervention.
* **Resource Optimization:** Maintained an $O(1)$ heap memory footprint during high-volume indexing using stream-based batch processing.

## 📁 Repository Structure

```text
DEV-SEARCH-ENGINE/
├── api/             # Express.js proxy for secure ES querying
├── client/          # React + Redux frontend
├── config/          # Kafka cluster configurations
├── scripts/         # Setup automation
├── workers/         # Kafka Consumers (Indexer) & Producers (Ingestors)
├── docker-compose.yml
└── .gitignore
📦 Getting Started
1. Prerequisites
Docker & Docker Desktop installed.

Node.js (v18+) installed.

2. Startup Sequence
Spin up Infrastructure:

Bash
docker-compose up -d
Initialize Kafka Topics:

Bash
node scripts/setup.js
Start the Indexer (Consumer):

Bash
node workers/indexer/index.js
Start the Ingestion Fleet:

Bash
node workers/ingestion/github.js
node workers/ingestion/stackoverflow.js
Start API Proxy & Frontend:

Bash
# In a new terminal
cd api && node server.js

# In another terminal
cd client && npm run dev
👨‍💻 Author
Agnimitra Sasaru

GitHub Profile

LinkedIn
