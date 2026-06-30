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
# 📦 Getting Started

## 1. Prerequisites

Before running the project, make sure you have the following installed:

- Docker & Docker Desktop
- Node.js (v18 or later)

---

## 2. Startup Sequence

Follow the steps below in the given order.

### Step 1: Spin Up the Infrastructure

```bash
docker-compose up -d
```

This starts all required services (Kafka, Zookeeper, Elasticsearch, etc.) in the background.

---

### Step 2: Initialize Kafka Topics

```bash
node scripts/setup.js
```

This creates all the required Kafka topics for the application.

---

### Step 3: Start the Indexer (Kafka Consumer)

```bash
node workers/indexer/index.js
```

The indexer consumes messages from Kafka and indexes them into Elasticsearch.

---

### Step 4: Start the Ingestion Workers

Run each worker in a separate terminal.

#### GitHub Ingestion Worker

```bash
node workers/ingestion/github.js
```

#### Stack Overflow Ingestion Worker

```bash
node workers/ingestion/stackoverflow.js
```

These workers continuously fetch data and publish it to Kafka.

---

### Step 5: Start the API Server

Open a new terminal and run:

```bash
cd api
node server.js
```

The API server provides endpoints for the frontend.

---

### Step 6: Start the Frontend

Open another terminal and run:

```bash
cd client
npm run dev
```

This starts the development server for the frontend application.

---

## 🚀 Project Startup Order

```text
1. docker-compose up -d
        ↓
2. node scripts/setup.js
        ↓
3. node workers/indexer/index.js
        ↓
4. node workers/ingestion/github.js
5. node workers/ingestion/stackoverflow.js
        ↓
6. cd api && node server.js
        ↓
7. cd client && npm run dev
```

Once all services are running successfully, open the frontend in your browser and start using the application.
👨‍💻 Author
Agnimitra Sasaru

GitHub Profile

LinkedIn
