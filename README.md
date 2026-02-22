# 🌏 Job Market Intelligence Platform — Southeast Asia

> A living, end-to-end data product that tracks, analyzes, and forecasts tech job market trends across Southeast Asia. Built with real MLOps practices, automated pipelines, and a live interactive dashboard.

---

## What This Platform Does

This platform continuously collects tech job postings from across Southeast Asia (Malaysia, Singapore, Indonesia), processes them through ML models, and surfaces actionable intelligence through a publicly accessible dashboard — updated automatically, no manual intervention required.

**Core Insights Delivered:**
- Skill demand trends and gap analysis (weekly)
- Company hiring velocity tracking (weekly)
- Role decay and emergence detection (monthly)
- Tech stack convergence clusters (monthly)
- Entry-level accessibility scoring (monthly)
- Remote opportunity tracking (weekly)

---

## Architecture Overview

```
[Job Boards / APIs] 
        ↓
[Collection Layer]  ← Airflow orchestrated, fault-tolerant scrapers
        ↓
[Raw Storage]       ← MinIO (S3-compatible) — original data preserved
        ↓
[Normalization]     ← Schema unification, deduplication, skill extraction
        ↓
[PostgreSQL + DuckDB] ← Structured storage + fast analytical queries
        ↓
[ML Engine]         ← Prophet forecasting, HDBSCAN clustering, NetworkX graphs
        ↓
[FastAPI Layer]     ← Clean REST API with Redis caching
        ↓
[Streamlit Dashboard] ← Live, interactive, publicly accessible
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Scraping | Python, BeautifulSoup4, Playwright |
| Data Quality | Great Expectations |
| Raw Storage | MinIO (S3-compatible) |
| Database | PostgreSQL 15 + DuckDB |
| ML Tracking | MLflow |
| ML Models | Prophet, LightGBM, HuggingFace Transformers, NetworkX |
| API | FastAPI + Redis |
| Dashboard | Streamlit + Plotly + Altair |
| Containerization | Docker + Docker Compose |
| Version Control | GitHub |

---

## Project Structure

```
job-market-platform/
├── airflow/
│   ├── dags/               ← Airflow DAG definitions
│   ├── plugins/            ← Custom Airflow operators/hooks
│   └── logs/               ← Airflow execution logs
├── src/
│   ├── collectors/         ← Source-specific scrapers
│   ├── normalizers/        ← Data cleaning & normalization
│   ├── storage/            ← DB models, S3 client, migrations
│   ├── models/             ← ML model training & inference
│   ├── api/                ← FastAPI application
│   ├── monitoring/         ← Model & pipeline monitoring
│   └── utils/              ← Shared utilities
├── config/                 ← Environment configs, skill taxonomy
├── docker/                 ← Dockerfiles
├── tests/                  ← Unit & integration tests
├── notebooks/              ← Exploratory analysis
└── docs/                   ← Architecture diagrams, data cards
```

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- 8GB RAM minimum (Airflow is memory hungry)

### Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/job-market-platform-sea.git
cd job-market-platform-sea

# Copy environment template
cp .env.example .env
# Edit .env with your configuration

# Start all services
docker-compose up -d

# Initialize the database
docker-compose exec app python -m src.storage.init_db

# Access services
# Airflow UI:   http://localhost:8080
# MinIO UI:     http://localhost:9001
# MLflow UI:    http://localhost:5000
# FastAPI docs: http://localhost:8000/docs
# Dashboard:    http://localhost:8501
```

---

## Development Phases

- **V1 (Current):** Pipeline + Storage + Core ML + Dashboard
- **V2:** Market Sentiment Pulse, Compensation Intelligence
- **V3:** Personalized job search, worldwide expansion

---

*Built by [Your Name] — Final Year Project, Bachelor of Computer Science (Data Science), Monash University*
