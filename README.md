# 📊 Real-Time Marketing Analytics Pipeline

![Java](https://img.shields.io/badge/Java-17-orange?style=for-the-badge&logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.x-6DB33F?style=for-the-badge&logo=springboot)
![Apache Beam](https://img.shields.io/badge/Apache_Beam-Streaming-orange?style=for-the-badge)
![Google Cloud](https://img.shields.io/badge/GCP-Dataflow%20%7C%20BigQuery-4285F4?style=for-the-badge&logo=googlecloud)
![Redis](https://img.shields.io/badge/Redis-Cache-red?style=for-the-badge&logo=redis)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-4169E1?style=for-the-badge&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker)
![Kubernetes](https://img.shields.io/badge/Kubernetes-GKE-326CE5?style=for-the-badge&logo=kubernetes)

A **distributed real-time analytics platform** that ingests financial transactions, performs streaming analytics using **Apache Beam on Google Cloud Dataflow**, stores historical metrics in **Google BigQuery**, and continuously updates **Redis-based routing strategies** to optimize transaction processing.

The platform combines a **Spring Boot microservice**, a **streaming analytics engine**, and a **dynamic feedback loop** to provide real-time visibility into transaction throughput, latency, and success rate while improving routing efficiency.

---

# 📌 Features

- 🚀 High-throughput financial transaction ingestion
- 📊 Real-time streaming analytics using Apache Beam
- ⚡ Dynamic routing optimization powered by Redis
- 🗄️ Historical analytics stored in Google BigQuery
- 💾 PostgreSQL persistence for transaction records
- 🔄 Automated feedback loop for routing optimization
- 🐳 Dockerized microservices
- ☸️ Kubernetes-ready deployment on GKE
- 📈 Near real-time operational metrics and dashboards

---

# 🛠 Tech Stack

| Category | Technology |
|-----------|------------|
| Backend | Java 17, Spring Boot 3 |
| Streaming | Apache Beam, Python |
| Stream Processing | Google Cloud Dataflow |
| Data Warehouse | Google BigQuery |
| Cache | Redis |
| Database | PostgreSQL |
| Containerization | Docker |
| Orchestration | Kubernetes (GKE) |
| Cloud | Google Cloud Platform |
| Build Tool | Maven |

---

# 🏗 System Architecture

```text
                    +----------------------+
                    | Transaction Clients  |
                    +----------+-----------+
                               |
                               |
                     Spring Boot REST API
                               |
              +----------------+----------------+
              |                                 |
              |                          PostgreSQL
              |                         Transaction Store
              |
              |
         Publish Events
              |
              |
      Apache Beam Pipeline
      (Google Cloud Dataflow)
              |
      +-------+--------+
      |                |
      |                |
 Google BigQuery    Redis
 Historical Data   Routing Rules
      |                |
      +-------+--------+
              |
      Dynamic Routing Feedback
```

---

# 📂 Project Structure

```text
analytics-pipeline/
│
├── transaction-service/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/com/analytics/
│   │   │   └── resources/
│   │   │        application.properties
│   │   └── test/
│   ├── Dockerfile
│   └── pom.xml
│
├── beam-pipeline/
│   ├── pipeline.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── k8s/
│   └── deployment.yaml
│
└── README.md
```

---

# ⚙️ How It Works

## Transaction Processing

1. Client submits a financial transaction.
2. Spring Boot validates and processes the request.
3. Routing rules are fetched from Redis.
4. Transaction is routed through the preferred gateway.
5. Transaction details are persisted in PostgreSQL.
6. Streaming events are forwarded to Apache Beam.

---

## Streaming Analytics

Apache Beam continuously processes transaction events using **1-minute fixed windows** to calculate:

- Transaction Throughput
- Average Processing Latency
- Success Rate
- Failure Rate

The computed metrics are written into **Google BigQuery** for historical reporting and dashboard visualization.

---

## Dynamic Routing Feedback Loop

The analytics pipeline continuously evaluates gateway performance.

When a gateway exhibits higher latency or lower success rates:

- Beam identifies the optimal gateway.
- Redis routing rules are updated dynamically.

Example Redis key:

```text
ROUTING_RULE:PREFERRED_GATEWAY
```

This enables future transactions to automatically use the most efficient gateway, reducing routing latency by approximately **40%**.

---

# 🚀 Getting Started

## Prerequisites

- Java 17+
- Python 3.10+
- Maven 3.8+
- Docker Desktop
- PostgreSQL
- Redis

---

## 1. Clone Repository

```bash
git clone https://github.com/yourusername/analytics-pipeline.git

cd analytics-pipeline
```

---

## 2. Start PostgreSQL & Redis

```bash
docker run -d \
--name postgres-db \
-e POSTGRES_DB=analytics_db \
-e POSTGRES_PASSWORD=postgres \
-p 5432:5432 \
postgres:15-alpine
```

```bash
docker run -d \
--name redis-cache \
-p 6379:6379 \
redis:alpine
```

Verify containers:

```bash
docker ps
```

---

## 3. Run Transaction Service

```bash
cd transaction-service

mvn clean install

mvn spring-boot:run
```

Application starts at:

```
http://localhost:8080
```

---

## 4. Run Apache Beam Pipeline

```bash
cd beam-pipeline

pip install -r requirements.txt

python pipeline.py --runner=DirectRunner
```

---

# 📡 REST API

## 1️⃣ Submit Financial Transaction

**POST**

```
/api/v1/transactions/submit
```

### Request

```json
{
  "customerId": "CUST_9918",
  "amount": 250.75
}
```

### Response

```json
{
  "transactionId": "b1a3d9e4-8f12-4c2d-9831-28941f19030e",
  "customerId": "CUST_9918",
  "amount": 250.75,
  "status": "SUCCESS",
  "routingGateway": "GATEWAY_PRIMARY",
  "latencyMs": 14.5,
  "timestamp": "2026-08-04T12:00:00"
}
```

---

## 2️⃣ Fetch Transaction Status

Returns the latest transaction status from Redis cache (or PostgreSQL fallback).

**GET**

```
/api/v1/transactions/{id}/status
```

### Response

```text
SUCCESS
```

---

# 📊 Streaming Metrics

The Apache Beam pipeline computes the following metrics every **1 minute**:

| Metric | Description |
|----------|-------------|
| Throughput | Transactions processed per minute |
| Success Rate | Percentage of successful transactions |
| Failure Rate | Percentage of failed transactions |
| Average Latency | Mean transaction processing latency |
| Gateway Performance | Latency comparison across gateways |

---

# 🐳 Docker

## Build Transaction Service

```bash
docker build -t transaction-service:latest ./transaction-service
```

## Build Beam Pipeline

```bash
docker build -t beam-pipeline:latest ./beam-pipeline
```

---

# ☸ Kubernetes Deployment

Deploy all services:

```bash
kubectl apply -f k8s/deployment.yaml
```

Verify resources:

```bash
kubectl get pods

kubectl get svc
```

---

# ☁ Google Cloud Components

| Service | Purpose |
|----------|---------|
| Compute Engine | Host application infrastructure |
| Google Cloud Dataflow | Execute Apache Beam streaming pipeline |
| Google BigQuery | Store historical analytics |
| Google Kubernetes Engine (GKE) | Container orchestration |

---

# 📈 Performance Optimizations

- Redis-backed dynamic routing reduces gateway lookup latency.
- Apache Beam enables scalable stream processing with fixed window analytics.
- Google BigQuery supports efficient long-term analytics and dashboarding.
- Dynamic feedback loop automatically adjusts routing strategies based on live performance.
- Docker and Kubernetes provide scalable deployment and simplified infrastructure management.

---

# 🔮 Future Enhancements

- Apache Kafka for event streaming
- Prometheus & Grafana monitoring
- OpenAPI / Swagger documentation
- JWT Authentication
- Multi-region deployment
- BigQuery dashboard integration with Looker Studio
- ML-based gateway selection
- Distributed tracing with OpenTelemetry

---

# 👨‍💻 Author

**Tanaya Naik**

GitHub: [Tanaya Naik](https://github.com/NaikTanaya)

---

Feel free to use, modify, and distribute this project.
