# 🛠️ Data Engineering Projects

> End-to-end data engineering projects — from a real-time streaming lakehouse and fraud-ring detector to a durable log built from scratch and a schema contract tool for Kafka pipelines.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Java](https://img.shields.io/badge/Java-ED8B00?style=flat-square&logo=openjdk&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Flink-E6526F?style=flat-square&logo=apacheflink&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Iceberg-1C1C1C?style=flat-square)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
[![CI](https://github.com/SEPURI-SAI-KRISHNA/Data-engineering-projects/actions/workflows/ci.yml/badge.svg)](https://github.com/SEPURI-SAI-KRISHNA/Data-engineering-projects/actions/workflows/ci.yml)

A collection of **end-to-end projects** that turn messy, high-throughput data into reliable, queryable systems, plus two systems built from scratch to understand the guarantees underneath: a durable log and a schema contract tool. Each project is self-contained and runnable.

## 📦 Projects

| # | Project | Key Technologies | Description |
|---|---|---|---|
| 1 | [Streaming Lakehouse Platform](streaming-lakehouse-platform/) | Kafka · Flink · Iceberg · Trino · Superset | End-to-end locally deployed lakehouse |
| 2 | [Fraud Ring Detection](fraud-detection/) | Kafka · Flink · Redis · Flask | Real-time cycle detection in payment graphs |
| 3 | [Data Refinement Engine](Data_refinement_engine/) | Python · Streamlit · OpenAI API | Configurable record transformation/validation/derivation UI |
| 4 | [Airflow DAG Builder](airflow_drag_drop/) | React · React Flow · Node.js | Visual drag-and-drop Airflow DAG composer |
| 5 | [Spark Simulator](spark_simulator/) | Python · Streamlit | Interactive Spark execution concept explorer |
| 6 | [Elasticsearch Builder](elasticsearch_builder/) | Python | Query assessor, NL-to-DSL generator, and optimizer |
| 7 | [minilog](minilog/) | Java · Maven | Durable log from scratch, crash-verified |
| 8 | [datactl](datactl/) | Python · YAML · Kafka · Iceberg | Schema contract tool — datasets as code |

### 1. Streaming Lakehouse Platform — [`streaming-lakehouse-platform/`](streaming-lakehouse-platform/)
A full real-time lakehouse organised as **ingestion → processing → storage**, containerised with Docker. Streams events in through Kafka, processes them with Flink, and lands them in Iceberg on MinIO for querying with Trino and visualising in Superset.

**Quick start:** `docker compose up -d` inside `streaming-lakehouse-platform/`.

### 2. Fraud Detection — [`fraud-detection/`](fraud-detection/)
Detects money-laundering rings (A → B → C → A) in a live transaction stream. Transactions flow through Kafka into a Flink job that builds a payment graph per time window and runs a DFS cycle detector. Alerts land in Redis; a Flask + vis.js dashboard renders rings as a live graph.

**Quick start:** `docker compose up -d` inside `fraud-detection/`, then run `datagen.py`.

### 3. Data Refinement Engine — [`Data_refinement_engine/`](Data_refinement_engine/)
Turns a **raw record into a refined record** by applying user-defined **transformations, validations, and derivations** — a configurable refinement layer with an AI assistant that can generate new processing functions from a plain-English description.

**Quick start:** `pip install -r requirements.txt && streamlit run Engine/engine.py`

### 4. Airflow Drag-and-Drop Builder — [`airflow_drag_drop/`](airflow_drag_drop/)
A **visual, drag-and-drop builder** for Apache Airflow DAGs — compose pipelines in a browser without hand-writing DAG code, then export the generated DAG definition as YAML.

**Quick start:** `cd airflow_drag_drop/airflow-builder && npm install && npm start`

### 5. Spark Simulator — [`spark_simulator/`](spark_simulator/)
An interactive Streamlit app that models a mini Spark cluster — Driver and Executors — so you can explore partitioning, shuffle, fault tolerance, OOM behaviour, and straggler effects hands-on. See its `How-to-run.md`.

**Quick start:** `pip install streamlit && streamlit run spark_simulator/main.py`

### 6. Elasticsearch Builder — [`elasticsearch_builder/`](elasticsearch_builder/)
Tooling for **Elasticsearch query engineering**: a static query assessor (heap/fan-out/anti-pattern checks), a natural-language-to-DSL generator, and an optimizer.

### 7. minilog — [`minilog/`](minilog/)
A **durable log built from scratch** in Java with an honest ack contract: if `append()` returned, the record survives `kill -9`. Ships with a design doc, deterministic corruption-recovery tests, and a chaos harness that crash-tests the writer thousands of times.

Key demonstrations:
- **Crash-safe recovery** — 25 SIGKILL rounds, 6,069 acked records, zero lost
- **Exactly-once processing** — atomic state + offset snapshots vs. naive split, proven both ways with a chaos harness
- **Throughput vs. durability** — fsync per append ≈ 400 rec/s; batching 100 records ≈ 21–26k rec/s at identical durability

**Tests:** `mvn test` · **Chaos harness:** `python3 chaos/chaos_run.py 100`

### 8. datactl — [`datactl/`](datactl/)
**Datasets as code**: declare a dataset's schema, Kafka topic, and Iceberg table once in a YAML spec; the tool reconciles infrastructure to match and **gates schema changes** that would break downstream consumers — with CI-friendly exit codes.

```bash
python3 -m datactl validate --specs specs
python3 -m datactl check-compat specs/orders.yaml examples/orders-v2.yaml
```

Exit codes are the CI contract: **0** clean, **1** invalid specs, **2** breaking change (needs a new dataset version). **Tests:** `python3 -m pytest` (pure-logic, no infrastructure needed).

## 📂 Repository Structure

```
Data-engineering-projects/
├── streaming-lakehouse-platform/   # Kafka + Flink + Iceberg + Trino lakehouse
├── fraud-detection/                # Real-time fraud-ring detector
├── Data_refinement_engine/         # Record transformation/validation UI
├── airflow_drag_drop/              # Visual Airflow DAG builder
├── spark_simulator/                # Interactive Spark concept explorer
├── elasticsearch_builder/          # Elasticsearch query tooling
├── minilog/                        # Durable log from scratch (Java)
└── datactl/                        # Schema contract tool for Kafka/Iceberg
```

## ⚙️ Prerequisites

| Requirement | Used by |
|---|---|
| Docker & Docker Compose | `streaming-lakehouse-platform`, `fraud-detection` |
| Python 3.10+ | all Python projects |
| Java 17+ & Maven | `minilog`, `fraud-detection` (Flink job) |
| Node.js 18+ | `airflow_drag_drop` |

Per-project setup instructions are in each folder's `README.md` or `How-to-run.md`.

## 🚀 Getting started

Each project is independent — `cd` into a project folder and follow its local docs. Most run with `docker-compose up` or a single Python entrypoint.

## 🗺️ Status

| Project | Status | Next milestone |
|---|---|---|
| Streaming Lakehouse | ✅ Complete | Multi-region replication patterns |
| Fraud Detection | ✅ Complete | Event-time windows + allowed lateness |
| datactl | 🚧 Phase 1 done | Phase 2: `plan`/`apply` against live Kafka + Iceberg |
| minilog | 🚧 Phases 1–3 done | Power-loss testing with `dm-flakey` |
| Data Refinement Engine | ✅ Complete | — |
| Airflow DAG Builder | ✅ Complete | DAG export + Airflow REST API integration |
| Spark Simulator | ✅ Complete | Additional execution model visualisations |

## 🤝 Contributing

Contributions, corrections, and new project ideas are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

A full milestone log is in [CHANGELOG.md](CHANGELOG.md). Licensed under the [MIT License](LICENSE) — free to use, adapt, and build on with attribution.

---

<sub>📂 Part of my data-engineering work — explore more at **[sepuri-sai-krishna.pages.dev](https://sepuri-sai-krishna.pages.dev)** · by [Sepuri Sai Krishna](https://github.com/SEPURI-SAI-KRISHNA)</sub>
