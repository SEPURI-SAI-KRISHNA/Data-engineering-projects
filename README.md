# 🛠️ Data Engineering Projects

> Production-grade data engineering — from a real-time streaming lakehouse and fraud-ring detector to a durable log built from scratch and a schema contract tool for Kafka pipelines.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Java](https://img.shields.io/badge/Java-ED8B00?style=flat-square&logo=openjdk&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Flink-E6526F?style=flat-square&logo=apacheflink&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Iceberg-1C1C1C?style=flat-square)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

A collection of **self-contained, end-to-end data engineering projects** that span the full spectrum of the discipline: real-time stream processing, lakehouse storage, fraud detection, schema governance, and distributed systems fundamentals — each built to run and each explaining the reasoning behind the design choices.

---

## 📋 Table of Contents

- [Purpose](#-purpose)
- [Projects](#-projects)
- [Repository Structure](#-repository-structure)
- [Prerequisites](#-prerequisites)
- [Getting Started](#-getting-started)
- [Who This Is For](#-who-this-is-for)
- [Roadmap](#-roadmap)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🎯 Purpose

Most data engineering tutorials stop at "it works on my laptop". These projects go further: they demonstrate **production patterns** — exactly-once semantics, schema evolution contracts, crash-safe durability, and live graph-based fraud detection — and they explain the trade-offs honestly. The goal is a portfolio of projects you can read, run, and learn from.

---

## 📦 Projects

| # | Project | Key Technologies | Description |
|---|---|---|---|
| 1 | [Streaming Lakehouse Platform](#1-streaming-lakehouse-platform) | Kafka · Flink · Iceberg · Trino · Superset | End-to-end locally deployed lakehouse |
| 2 | [Fraud Ring Detection](#2-fraud-ring-detection) | Kafka · Flink · Redis · Flask | Real-time cycle detection in payment graphs |
| 3 | [datactl](#3-datactl) | Python · YAML · Kafka · Iceberg | Schema contract tool — datasets as code |
| 4 | [minilog](#4-minilog) | Java · Maven | Durable log from scratch, crash-verified |
| 5 | [Data Refinement Engine](#5-data-refinement-engine) | Python · Streamlit · OpenAI API | Interactive data cleaning and enrichment UI |
| 6 | [Airflow DAG Builder](#6-airflow-dag-builder) | React · React Flow · Node.js | Visual drag-and-drop Airflow DAG composer |
| 7 | [Spark Simulator](#7-spark-simulator) | Python · Streamlit | Interactive Spark execution concept explorer |

---

### 1. Streaming Lakehouse Platform — [`streaming-lakehouse-platform/`](streaming-lakehouse-platform/README.md)

An end-to-end, locally deployed distributed data platform demonstrating high-throughput event streaming, stateful stream processing, and open-table federated querying.

| Layer | Technology |
|---|---|
| Ingestion | Python producer with `msgspec` serialisation |
| Message broker | Apache Kafka (3 partitions, Murmur2 key hashing) |
| Stream processing | Apache Flink with tumbling windows & exactly-once |
| Storage | Apache Iceberg on MinIO (S3-compatible) |
| Metadata catalog | Project Nessie |
| Query engine | Trino |
| Visualisation | Apache Superset |

**Quick start:** `docker compose up -d` inside `streaming-lakehouse-platform/`.

---

### 2. Fraud Ring Detection — [`fraud-detection/`](fraud-detection/README.md)

Detects money-laundering rings (A → B → C → A) in a live transaction stream. Transactions flow through Kafka into a Flink job that builds a payment graph per time window and runs a DFS cycle detector. Alerts land in Redis; a Flask + vis.js dashboard renders rings as a live graph.

```
datagen.py → Kafka → Flink (CycleDetector) → Redis pub/sub → Flask + vis.js
```

**Quick start:** `docker compose up -d` inside `fraud-detection/`, then run `datagen.py`.

---

### 3. datactl — [`datactl/`](datactl/README.md)

*Datasets as code.* Each dataset — its schema, its Kafka topic, its Iceberg table — is declared once in a YAML spec that lives in git. `datactl` makes the infrastructure match the spec and refuses schema changes that would break consumers. Unlike general IaC it understands the one thing it manages: it can tell you *this change is a safe widening* or *this change strands every query naming that column*, at plan time, in the PR.

```bash
# Validate all specs in a directory
python3 -m datactl validate --specs specs

# Check schema compatibility between two versions
python3 -m datactl check-compat specs/orders.yaml examples/orders-v2.yaml
```

Exit codes are the CI contract: **0** clean, **1** invalid specs, **2** breaking change (needs a new dataset version).

**Tests:** `python3 -m pytest` (51 pure-logic tests, no infrastructure needed).

---

### 4. minilog — [`minilog/`](minilog/README.md)

A single-node durable log built from scratch in Java with an honest ack contract: **if `append()` returned, the record survives `kill -9`.** Built to understand what systems like Kafka actually promise and what it costs to keep the promise.

Key demonstrations:
- **Crash-safe recovery** — 25 SIGKILL rounds, 6 069 acked records, zero lost
- **Exactly-once processing** — atomic state + offset snapshots vs naive split (proven both ways with a chaos harness)
- **Throughput vs durability** — fsync per append ≈ 400 rec/s; batching 100 records ≈ 21–26k rec/s at identical durability

**Tests:** `mvn test` · **Chaos harness:** `python3 chaos/chaos_run.py 100`

---

### 5. Data Refinement Engine — [`Data_refinement_engine/`](Data_refinement_engine/README.md)

A configurable Streamlit app that turns raw records into refined records by applying user-defined **transformations, validations, and derivations** — a reusable refinement layer for cleaning and enriching data before it enters a pipeline. Includes an AI assistant that can generate new processing functions from a plain-English description.

**Quick start:** `pip install -r requirements.txt && streamlit run Engine/engine.py`

---

### 6. Airflow DAG Builder — [`airflow_drag_drop/`](airflow_drag_drop/README.md)

A **visual, drag-and-drop builder** for Apache Airflow DAGs — compose pipelines in a browser without hand-writing DAG code, then export the generated DAG definition as YAML. Built with React and React Flow.

**Quick start:** `cd airflow_drag_drop/airflow-builder && npm install && npm start`

---

### 7. Spark Simulator — [`spark_simulator/`](spark_simulator/README.md)

An interactive Streamlit app that models a mini Spark cluster — Driver and Executors — so you can explore partitioning, shuffle, fault tolerance, OOM behaviour, and straggler effects hands-on.

**Quick start:** `pip install streamlit && streamlit run spark_simulator/main.py`

---

## 📂 Repository Structure

```
Data-engineering-projects/
├── streaming-lakehouse-platform/   # Kafka + Flink + Iceberg + Trino lakehouse
│   ├── ingestion/
│   ├── processing/
│   ├── storage/
│   └── docker/
├── fraud-detection/                # Real-time fraud-ring detector
│   ├── fraud-ring/                 # Flink job (Java/Maven)
│   ├── dashboard/                  # Flask + vis.js frontend
│   └── datagen.py
├── datactl/                        # Schema contract tool for Kafka/Iceberg
│   ├── datactl/                    # Python package (spec, schema, compat, cli)
│   ├── specs/                      # Example dataset specs
│   └── DESIGN.md
├── minilog/                        # Durable log from scratch (Java)
│   ├── src/
│   ├── chaos/                      # Kill-9 & exactly-once chaos harnesses
│   ├── DESIGN.md
│   └── BENCHMARKS.md
├── Data_refinement_engine/         # Record transformation/validation UI
│   └── Engine/
├── airflow_drag_drop/              # Visual Airflow DAG builder
│   └── airflow-builder/
└── spark_simulator/                # Interactive Spark concept explorer
```

---

## ⚙️ Prerequisites

Requirements vary by project. The general dependencies are:

| Requirement | Used by |
|---|---|
| Docker & Docker Compose | `streaming-lakehouse-platform`, `fraud-detection` |
| Python 3.10+ | all Python projects |
| Java 17+ & Maven | `minilog`, `fraud-detection` (Flink job) |
| Node.js 18+ | `airflow_drag_drop` |

Per-project setup instructions are in each folder's `README.md` or `How-to-run.md`.

---

## 🚀 Getting Started

Each project is fully independent. Pick one, `cd` in, and follow the local docs:

```bash
# Example: start the streaming lakehouse
cd streaming-lakehouse-platform
docker compose up -d

# Example: run datactl tests
cd datactl
python3 -m venv venv && venv/bin/pip install pytest pyyaml
venv/bin/python -m pytest

# Example: run minilog chaos harness
cd minilog
mvn package -q
python3 chaos/chaos_run.py 100
```

---

## 👤 Who This Is For

- **Data engineers** looking for reference implementations of production patterns
- **Backend engineers** interested in distributed systems fundamentals (durable storage, stream processing, schema governance)
- **Interview candidates** preparing for system design or data engineering interviews
- **Students and practitioners** who learn best from runnable, explained code

---

## 🗺️ Roadmap

| Project | Status | Next milestone |
|---|---|---|
| Streaming Lakehouse | ✅ Complete | Multi-region replication patterns |
| Fraud Detection | ✅ Complete | Event-time windows + allowed lateness |
| datactl | 🚧 Phase 1 done | Phase 2: `plan`/`apply` against live Kafka + Iceberg |
| minilog | 🚧 Phases 1–3 done | Power-loss testing with `dm-flakey` |
| Data Refinement Engine | ✅ Complete | — |
| Airflow DAG Builder | ✅ Complete | DAG export + Airflow REST API integration |
| Spark Simulator | ✅ Complete | Additional execution model visualisations |

---

## 🤝 Contributing

Contributions, corrections, and new project ideas are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

---

## 📄 License

This repository is licensed under the [MIT License](LICENSE). You are free to use, adapt, and build on this work with attribution.

---

<sub>📂 Part of my data-engineering work — explore more at **[sepuri-sai-krishna.pages.dev](https://sepuri-sai-krishna.pages.dev)** · by [Sepuri Sai Krishna](https://github.com/SEPURI-SAI-KRISHNA)</sub>
