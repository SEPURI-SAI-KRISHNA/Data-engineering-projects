# 🛠️ Data Engineering Projects

> Production-grade data engineering — from a real-time streaming lakehouse to a fraud-ring detector and an AI-assisted record-refinement engine.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

A collection of **end-to-end projects** that turn messy, high-throughput data into reliable, queryable systems. Each one is self-contained and runnable.

## 📦 Projects

### 1. Streaming Lakehouse Platform — [`streaming-lakehouse-platform/`](streaming-lakehouse-platform/)
A full real-time lakehouse organised as **ingestion → processing → storage**, containerised with Docker. Streams events in, processes them, and lands them in a lakehouse for analytics.

### 2. Fraud Detection — [`fraud-detection/`](fraud-detection/)
Detects **fraud rings** from transaction streams. Ships with a data generator (`datagen.py`), a processing pipeline, and a dashboard — all runnable via `docker-compose`.

### 3. Data Refinement Engine — [`Data_refinement_engine/`](Data_refinement_engine/)
Turns a **raw record into a refined record** by applying user-defined **transformations, validations, and derivations** — a configurable refinement layer.

### 4. Airflow Drag-and-Drop Builder — [`airflow_drag_drop/`](airflow_drag_drop/)
A **visual, drag-and-drop builder** for Apache Airflow DAGs — compose pipelines without hand-writing DAG code.

### 5. Spark Simulator — [`spark_simulator/`](spark_simulator/)
A lightweight **simulator** to experiment with and understand Spark execution. See its `How-to-run.md`.

## 🚀 Getting started

Each project is independent — `cd` into a project folder and follow its local `README.md` / `How-to-run.md`. Most run with `docker-compose up` or a single Python entrypoint.

---

<sub>📂 Part of my data-engineering work — explore more at **[sepuri-sai-krishna.pages.dev](https://sepuri-sai-krishna.pages.dev)** · by [Sepuri Sai Krishna](https://github.com/SEPURI-SAI-KRISHNA)</sub>
