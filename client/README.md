# Universe Search Engine 🌌

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)]()
[![Node Version](https://img.shields.io/badge/Node-v18%2B-green)]()

A high-throughput, distributed data platform engineered to backfill, stream, and index millions of developer issues from GitHub and Stack Overflow. This project demonstrates large-scale systems engineering, event-driven architecture, distributed messaging, and scalable full-text search.

---

# 🏗️ System Architecture

The system is designed as a decoupled, event-driven microservices architecture where ingestion, messaging, indexing, and querying are isolated into independent services for scalability and fault tolerance.

> *(Add your architecture diagram here)*

---

# 🛠 Tech Stack

| Layer | Technology |
|--------|------------|
| Backend | Node.js, Express.js |
| Data Ingestion | Axios, REST APIs |
| Event Streaming | Apache Kafka |
| Search Engine | Elasticsearch |
| Frontend | React.js, Redux Toolkit, Vite |
| Infrastructure | Docker, Docker Compose |

---

# 💡 Key Engineering Challenges Solved

### 🚀 Rate-Limit Resilience

Implemented a **Recursive Divide & Conquer** strategy to bypass GitHub's 1000-result pagination limit by recursively splitting time windows until every issue is retrieved.

### ⚡ Backpressure Handling

Used Kafka partitions and `eachBatch` consumers to decouple ingestion from indexing, allowing the system to absorb sudden traffic spikes.

### 🛡 Fault Tolerance

Designed a **Dead Letter Queue (DLQ)** that automatically redirects malformed events without interrupting the indexing pipeline.

### 📦 Memory Optimization

Maintained an **O(1)** heap memory footprint using streaming batch processing during Elasticsearch indexing.

---

# 📁 Repository Structure

```text
DEV-SEARCH-ENGINE/
│
├── api/                     # Express API proxy
├── client/                  # React frontend
├── config/                  # Kafka configuration
├── scripts/                 # Setup scripts
├── workers/
│   ├── ingestion/
│   │   ├── github.js
│   │   └── stackoverflow.js
│   │
│   └── indexer/
│       └── index.js
│
├── docker-compose.yml
├── package.json
└── README.md
```

---

# 📦 Getting Started

## Prerequisites

Install the following before starting the project:

- Docker
- Docker Desktop
- Node.js (v18 or later)

---

## Startup Sequence

### 1. Start Infrastructure

```bash
docker-compose up -d
```

Starts Kafka, Zookeeper, Elasticsearch, and all required infrastructure.

---

### 2. Create Kafka Topics

```bash
node scripts/setup.js
```

Initializes all Kafka topics used by the application.

---

### 3. Start the Elasticsearch Indexer

```bash
node workers/indexer/index.js
```

Consumes Kafka events and indexes them into Elasticsearch.

---

### 4. Start GitHub Ingestion Worker

```bash
node workers/ingestion/github.js
```

Fetches GitHub issues and publishes them to Kafka.

---

### 5. Start Stack Overflow Ingestion Worker

```bash
node workers/ingestion/stackoverflow.js
```

Fetches Stack Overflow questions and publishes them to Kafka.

---

### 6. Start the API Server

Open another terminal.

```bash
cd api
node server.js
```

Runs the Express API.

---

### 7. Start the Frontend

Open another terminal.

```bash
cd client
npm run dev
```

Starts the React application.

---

# 🚀 Startup Flow

```text
docker-compose up -d
        │
        ▼
node scripts/setup.js
        │
        ▼
node workers/indexer/index.js
        │
        ▼
node workers/ingestion/github.js

node workers/ingestion/stackoverflow.js
        │
        ▼
cd api && node server.js
        │
        ▼
cd client && npm run dev
```

---

# ✨ Features

- High-throughput data ingestion
- Distributed event-driven architecture
- Recursive GitHub crawler
- Kafka-based buffering
- Elasticsearch full-text search
- Dead Letter Queue (DLQ)
- Fault-tolerant indexing
- React + Redux frontend
- Dockerized deployment

---

# 👨‍💻 Author

**Agnimitra Sasaru**

- GitHub: https://github.com/<HyphenAlpha456>
- LinkedIn: https://linkedin.com/in/<agnimitra-sasaru-097974274>

---

# 📄 License

This project is licensed under the **MIT License**.
