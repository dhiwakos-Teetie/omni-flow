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
