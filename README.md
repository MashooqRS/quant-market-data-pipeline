# Quant Market Data Pipeline – Statistical Arbitrage Data Foundation (5-Minute Bars)

## Overview
This project implements an end-to-end batch data engineering pipeline designed to support
statistical arbitrage research using **5-minute U.S. equity market data**. The pipeline ingests
high-frequency market data, performs validation and transformations, and prepares clean,
analytics-ready datasets for downstream **pair-based spread and Z-score analysis**.

The primary goal of the project is to **provide reliable, time-aligned market data**
that can be consumed by **quant researchers, analysts, and other downstream stakeholders** —
not to execute trades or deploy production trading signals.

---

## Objective
The pipeline is built to support statistical arbitrage workflows, including:
- Preparing consistent 5-minute price series for correlated equity pairs
- Ensuring time alignment and completeness across symbols
- Enabling spread calculation and Z-score–based deviation analysis using rolling windows

All downstream modeling and research depends on **data correctness, alignment, and availability**,
which is the core focus of this project.

---

## Architecture
**Ingestion**
- Market data fetched from Alpaca APIs
- 5-minute bars constructed and scheduled using Apache Airflow

**Processing & Validation**
- Timestamp normalization (UTC → ET)
- Cross-symbol alignment of 5-minute bars
- Missing bar and completeness checks
- Duplicate prevention and schema validation

**Storage**
- PostgreSQL used as the analytics-ready datastore for statistical arbitrage research

**Access & Exploration**
- Metabase connected for ad-hoc querying and data inspection by analysts
- Streamlit + Plotly used for lightweight exploratory time-series and spread visualization during validation

---

## Project Structure

quant_data_pipeline/
├── dags/ # Airflow DAG definitions
│ └── quant_pipeline_dag.py
├── project/
│ ├── ingest_data.py # Market data ingestion logic
│ ├── transform_data.py # Validation, alignment, prep logic
│ └── validate_data.py
├── docker-compose.yml # Local orchestration
├── Dockerfile
├── Dockerfile.streamlit
├── requirements.txt
├── requirements_streamlit.txt
└── README.md


---

## Pipeline Flow
1. Airflow DAG runs on a scheduled basis aligned with U.S. market timing
2. Market data is ingested and aggregated into 5-minute bars
3. Data is validated for:
   - Missing bars
   - Timestamp alignment across symbols
   - Duplicate records
4. Cleaned and aligned data is loaded into PostgreSQL
5. Data becomes available for downstream pair analysis and Z-score research

---

## Data Quality Checks
- Missing 5-minute bar detection per symbol and trading day
- Timestamp normalization and cross-symbol alignment
- Duplicate record prevention
- Row count and load validation

These checks ensure the resulting datasets are suitable for statistical arbitrage research,
where small inconsistencies can materially impact spread and Z-score calculations.

---

## Technology Stack
- **Languages**: Python, SQL
- **Orchestration**: Apache Airflow
- **Database**: PostgreSQL
- **Containerization**: Docker
- **Data Access & Exploration**:
  - Metabase (ad-hoc querying and inspection)
  - Streamlit + Plotly (exploratory visualization)

---

## How to Run Locally
Prerequisites:
- Docker & Docker Compose

Start the pipeline:
```bash
docker compose up -d

