# 🌊 Omni-Flow: Production Data Architecture & DDIA Simulation Suite

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Flink](https://img.shields.io/badge/Apache_Flink-1.19-E6526F?style=for-the-badge&logo=apacheflink&logoColor=white)](https://flink.apache.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-OLAP-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=black)](https://clickhouse.com/)
[![Debezium](https://img.shields.io/badge/Debezium-CDC-00A88F?style=for-the-badge)](https://debezium.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

**Omni-Flow** is a hands-on, production-grade Data Engineering and Distributed Systems lab suite inspired by all **12 chapters of *Designing Data-Intensive Applications* (DDIA)**. It demonstrates end-to-end event streaming, real-time stateful stream processing, Change Data Capture (CDC), Hot/Cold Lakehouse tiering, automated data quality gates, and binary encoding internals.

---

## 🏗️ System Architecture Overview

```mermaid
flowchart TD
    PG[(PostgreSQL\nWAL & Outbox)] -->|Debezium CDC| K[Apache Kafka\nKRaft 3 Partitions]
    K -->|Real-Time Ingestion| CH[(ClickHouse\nReplacingMergeTree)]
    K -->|Event Time & 5s Watermark| FL[Apache Flink 1.19\n10s Tumbling Window]
    CH <-->|Hot / Cold Tiering| S3[(MinIO S3\nParquet Cold Storage)]
    
    subgraph Orchestration & Quality
        DAG[Medallion DAG] --> BR[Bronze] --> DQ{DQ Gate}
        DQ -->|Pass| SL[Silver] --> GL[Gold]
        DQ -->|Fail| QR[Quarantine]
    end
---

## 🧪 Hands-On Labs (6 Production Simulations)

| Lab / Module | Port | Core Technologies | Architectural Focus & DDIA Concepts |
| :--- | :---: | :--- | :--- |
| **`omni-command-center`** | `8000` | Kafka KRaft (3 Partitions), ClickHouse (`ReplacingMergeTree`) | High-throughput ingestion, idempotency, latency benchmarking (`p50`/`p95`/`p99`), and Chaos Testing |
| **`lab1-flink-stream`** | `8081` | Apache Flink 1.19, Flink SQL | Stateful stream processing, Event Time vs. Processing Time, 5s Watermarks, 10s Tumbling Windows, Fraud Detection |
| **`lab2-minio-lakehouse`** | `9001` | ClickHouse, MinIO (S3), Parquet | Hot/Cold storage tiering, columnar compression, and Snapshot Time-Travel queries |
| **`lab3-debezium-cdc`** | `8083` | PostgreSQL (WAL), Debezium CDC, Kafka, ClickHouse | Log-based Change Data Capture, Transactional Outbox Pattern, and eventual consistency |
| **`lab4-orchestration-audit`** | `8080` | Medallion Architecture, Python | Bronze ➔ Automated DQ Gate / Quarantine ➔ Silver ➔ Gold pipeline with key-level data lineage & auditing |
| **`lab5-encoding-evolution`** | `8085` | Python Web Playground (No Docker required) | Byte-level inspection of JSON, Protobuf, Avro, and Thrift; schema evolution; interactive simulation covering all 12 DDIA chapters |

---

## 📚 Documentation & Study Guides

* **`docs/DDIA_MASTER_GUIDE.md`** — Comprehensive mapping of all 12 DDIA chapters to hands-on implementations across the 6 labs.
* **`docs/INTERVIEW_SCRIPT_THAI_ENG.md`** — Bilingual (English & Thai) system design and technical interview scripts structured around *Problem ➔ Solution & Impact*.

---

## 🚀 Quick Start (Interactive 12-Chapter Playground)

To explore the byte-level encoding analyzer and interactive 12-chapter DDIA simulations without spinning up Docker containers:

```bash
cd lab5-encoding-evolution
python encoding_app.py
