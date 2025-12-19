## 0. Problem Statement & Business Impact

### Problem

Modern public transit systems generate large volumes of data (vehicle telemetry, schedules, delays), but this data is often siloed across real-time and batch systems, underutilized for proactive decision-making, and consumed only after service degradation has already occurred.

As a result, transit agencies struggle with unreliable service, inefficient route planning, and reduced rider satisfaction — despite having access to rich operational data.

### Solution

**SmartRide (TransLink Edition)** is an end-to-end real-time and batch analytics platform that models Vancouver’s public transit system using TransLink data.

The platform combines:
- Real-time vehicle telemetry ingestion and processing
- Batch ingestion of GTFS static schedules
- A cloud-based data lake for raw and curated datasets
- Analytics-ready data models for transit performance analysis

This architecture reflects modern industry patterns used in mobility, logistics, and large-scale data platforms.

### Business Impact

SmartRide enables transit operators and planners to:
- Detect and respond to service disruptions faster
- Identify chronically delayed routes and trips
- Analyze route-level performance trends
- Make data-backed operational and planning decisions

The platform improves operational visibility, service reliability, and long-term planning outcomes.

---

## 1. Architecture Overview

SmartRide is designed as a modular, scalable data platform that separates real-time and batch workloads while sharing a common storage layer.

### High-Level Flow

1. **Real-Time Ingestion**
   - Simulated vehicle telemetry events are produced to Kafka topics.
   - Apache Spark Structured Streaming consumes these events in near real time.

2. **Raw Data Lake Storage**
   - Streaming outputs are written to Amazon S3 in Parquet format.
   - Data is partitioned by ingestion time to support downstream analytics.

3. **Batch Ingestion (GTFS Static Data)**
   - Apache Airflow orchestrates daily ingestion of TransLink GTFS static datasets.
   - GTFS files are downloaded, extracted, and stored in S3 as raw reference data.

4. **Transformation & Analytics Layer**
   - Spark batch jobs transform raw GTFS and telemetry data into analytics-ready tables.
   - Aggregated models such as delay metrics and route performance are produced.

5. **Analytics & Visualization**
   - Curated datasets are exposed to BI tools for dashboarding and reporting.

### Design Principles

- Separation of streaming and batch pipelines
- Cloud-native, object storage–based data lake
- Schema-aware, columnar storage (Parquet)
- Orchestration-driven batch processing
- Scalable, fault-tolerant components

---

## 2. Tech Stack

### Data Ingestion & Streaming
- **Apache Kafka** — event streaming platform for real-time telemetry
- **Python Kafka Producer** — simulated vehicle telemetry generation

### Data Processing
- **Apache Spark (Structured Streaming & Batch)** — real-time and batch transformations
- **PySpark** — unified processing layer for streaming and static datasets

### Orchestration
- **Apache Airflow** — scheduling and orchestration of GTFS batch pipelines
- **PostgreSQL** — Airflow metadata database

### Storage
- **Amazon S3** — raw and curated data lake storage
- **Parquet** — columnar, analytics-optimized storage format

### Infrastructure & DevOps
- **Docker & Docker Compose** — local orchestration of Kafka, Spark, and Airflow
- **AWS SDK (boto3)** — programmatic interaction with S3

### Analytics & Visualization
- **Power BI** — dashboards and reporting (planned)

---