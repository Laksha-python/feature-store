# 🚀 Real-Time Feature Store Platform

A feature store platform built using Kafka, FastAPI, PostgreSQL, Redis, Airflow, and Streamlit.

The system ingests user events, computes behavioral features, stores them in both online and offline stores, tracks feature metadata, and exposes feature-serving APIs for downstream applications.

---

# 📌 Problem Statement

Machine learning systems require consistent, reusable, and low-latency features for both training and inference.

Without a centralized feature platform:

- Feature logic becomes duplicated across teams
- Historical feature values are difficult to manage
- Feature freshness is hard to monitor
- Online and offline data become inconsistent
- Feature lineage and schema changes are difficult to track

This project demonstrates how a feature store can solve these challenges using an event-driven architecture.

---

# 🏗️ Architecture

![Architecture](docs/architecture.png)

### System Flow

```text
Event Producer
      │
      ▼
    Kafka
      │
      ▼
Feature Materialization
      │
      ├────────► PostgreSQL (Offline Store)
      │
      ├────────► Redis (Online Store)
      │
      └────────► Metadata Layer
                        │
                        ▼
                    FastAPI
                        │
                        ▼
                   Streamlit UI
```

---

# ✨ Key Features

## Event Ingestion

- Kafka-based event streaming
- Purchase, refund, and view event processing
- Event validation pipeline
- Dead Letter Queue (DLQ) support

## Feature Computation

### User Features

- Rolling 7-day purchase count
- Rolling 30-day spend
- Recency days
- Net revenue (30-day)

### Product Features

- Rolling 1-hour sales
- Rolling 24-hour sales
- Conversion rate
- Refund rate

## Offline Feature Store

Implemented using PostgreSQL.

Supports:

- Historical feature storage
- Batch analytics
- Feature backfills
- Durable persistence

## Online Feature Store

Implemented using Redis.

Supports:

- Low-latency feature access
- Cached feature serving
- Fast API retrieval

## Metadata Governance

Tracks:

- Feature registry
- Schema history
- Feature lineage
- Feature freshness

## Feature Serving API

Built using FastAPI.

Provides:

- User feature retrieval
- Product feature retrieval
- Metadata endpoints
- Health checks

## Workflow Orchestration

Built using Apache Airflow.

Supports:

- Scheduled feature pipelines
- Feature refresh workflows
- Monitoring jobs

---

# 🛠️ Tech Stack

| Layer | Technology |
|---------|------------|
| Streaming | Apache Kafka |
| API Layer | FastAPI |
| Offline Store | PostgreSQL |
| Online Store | Redis |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit |
| Containerization | Docker |
| Language | Python |

---

# 📊 Dashboard Screenshots

## Control Plane

![Control Plane](docs/screenshots/control-plane.png)

## Analytics Dashboard

![Analytics](docs/screenshots/analytics.png)

## Feature Monitoring

![Feature Monitoring](docs/screenshots/features.png)

## API Documentation

![Swagger UI](docs/screenshots/swagger-ui.png)

---

# 📁 Project Structure

```text
feature-store/
│
├── api/                      
├── ingestion/                
├── processing/               
├── storage/                  
├── stream_processing/        
├── airflow_orchestration/    
├── ui/                       
├── docs/                     
├── tests/                    
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# 🔄 Example Feature Flow

### Incoming Event

```json
{
  "event_type": "purchase",
  "user_id": "user_5",
  "product_id": "product_1",
  "price": 838
}
```

### Computed Features

```json
{
  "rolling_7d_purchase_count": 5,
  "rolling_30d_spend": 2840,
  "recency_days": 1,
  "net_revenue_30d": 2500
}
```

### Data Flow

```text
Kafka
  ↓
Feature Materialization
  ↓
PostgreSQL
  ↓
Redis
  ↓
FastAPI
  ↓
Dashboard
```

---

# 🚀 Running the Project

## Clone Repository

```bash
git clone https://github.com/Laksha-python/feature-store.git
cd feature-store
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Start Infrastructure

```bash
docker-compose up -d
```

This starts:

- Kafka
- PostgreSQL
- Redis
- Airflow

## Run Feature Materialization

```bash
python ingestion/feature_materialization_job.py
```

## Start FastAPI

```bash
uvicorn api.main:app --reload
```

API Docs:

```text
http://localhost:8000/docs
```

## Start Streamlit Dashboard

```bash
streamlit run ui/app.py
```

Dashboard:

```text
http://localhost:8501
```

---

# 🎯 Engineering Concepts Demonstrated

- Event-Driven Architecture
- Streaming Data Pipelines
- Online & Offline Feature Stores
- Feature Materialization
- Metadata Management
- Data Lineage Tracking
- Schema Evolution Monitoring
- Feature Freshness Tracking
- Workflow Orchestration
- Backend API Development
- Distributed System Fundamentals

---

# 🔮 Future Improvements

- Real-time Kafka consumers
- Feature versioning
- Feature access controls
- Automated data quality checks
- Prometheus & Grafana monitoring
- CI/CD pipeline
- Kubernetes deployment

---

# 👨‍💻 Author

**Laksha K**

Data Engineering • Backend Engineering • Data Platforms

---
⭐ If you found this project interesting, feel free to star the repository.