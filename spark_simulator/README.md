# Spark Simulator

An interactive Streamlit application for exploring and understanding Apache Spark execution concepts. The simulator models a mini Spark cluster — a Driver coordinating multiple Executors — and lets you trigger real operations (load, filter, group-by, join, sort) while watching data move between nodes in real time.

## What it simulates

The app models the core components of a Spark cluster:

- **Driver** — orchestrates the job, splits data into partitions, collects results, and writes a timestamped event log.
- **Executors (×3)** — each holds a partition of the data. Executor state is colour-coded: blue (ALIVE), orange (STRAGGLER — slow I/O), red (OOM — out of memory), grey (DEAD — crashed).

You can explore these Spark concepts interactively:

| Concept | How to trigger |
|---|---|
| Data partitioning | Load data and watch it split across executors |
| Transformations | Filter, group-by (reduce), join, sort — each redistributes data across nodes |
| Fault tolerance / lineage | Kill an executor, then recover it via lineage re-computation |
| Out-of-memory behaviour | Set a low memory limit and load more rows than it allows |
| Straggler effect | Mark an executor as slow and observe delayed task completion |
| Shuffle | Join and sort operations visualise the shuffle step between executors |

## Directory layout

```
spark_simulator/
├── main.py          # Streamlit app — Driver/Executor model, UI, animations
└── How-to-run.md    # Minimal run instructions
```

## Quick start

```bash
cd spark_simulator
pip install streamlit pandas numpy
streamlit run main.py
```

Open the URL printed by Streamlit (default `http://localhost:8501`).

## How it fits into this repository

The Spark Simulator is a learning and demonstration tool. It makes abstract distributed-computing concepts (partitioning, shuffle, fault tolerance, memory pressure) visible and interactive, which complements the more production-oriented projects in this repository such as the [Streaming Lakehouse Platform](../streaming-lakehouse-platform/README.md) and [Fraud Ring Detection](../fraud-detection/README.md).

---

> Part of [Data Engineering Projects](../README.md) · [Contributing](../CONTRIBUTING.md) · [MIT License](../LICENSE)
