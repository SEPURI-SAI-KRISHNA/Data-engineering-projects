# Changelog

All notable milestones and additions to this repository are documented here.  
This project does not follow semantic versioning — entries are organised by milestone, not release number.

---

## [Unreleased]

- CI badge for `datactl` and `minilog` tests (pending first green run)
- Phase 2 of `datactl`: `plan`/`apply` against a live Kafka + Iceberg stack
- Phase 4 of `minilog`: power-loss testing with `dm-flakey`

---

## [2025-07] — Repository professionalism pass

- Added GitHub Actions CI workflow running `datactl` pytest and `minilog` Maven tests on every push/PR
- Added `.github/CODEOWNERS`, PR template, and issue templates (bug report, feature request)
- Added `SECURITY.md` (responsible disclosure policy) and `CODE_OF_CONDUCT.md`
- Added directory-level `README.md` files for `Data_refinement_engine/`, `airflow_drag_drop/`, and `spark_simulator/`
- Updated root `README.md` with a project index table and direct links to each project README

---

## [2025-Q2] — minilog phases 1–3 complete

- **Log core** (`Log`, `LogSegment`): append, read, crash-safe recovery with torn-record detection
- **Consumer + OffsetStore**: durable committed offsets, group commit, sparse index
- **Benchmarks**: fsync-per-append ≈ 400 rec/s; batch-100 ≈ 21–26k rec/s; recovery scan ≈ 221 MB/s
- **Chaos harness** (`chaos_run.py`): 25 SIGKILL rounds, 6 069 acked records, zero lost
- **Exactly-once demo** (`chaos_exactly_once.py`): atomic snapshot vs naive split, both ways proven

---

## [2025-Q1] — datactl phase 1 complete

- Strict YAML spec model (`DatasetSpec`, `StreamSpec`, `TableSpec`, `Schema`)
- Type system with safe-widening rules (`schema.py`)
- Schema compatibility gate: breaking vs safe changes, consumer-impact explanations (`compat.py`)
- CLI: `validate` and `check-compat` with CI-contract exit codes (0 / 1 / 2)
- 51 pure-logic tests, no infrastructure required

---

## [2024-Q4] — Initial projects

- **Streaming Lakehouse Platform**: Kafka → Flink (exactly-once, tumbling windows) → Iceberg on MinIO → Trino → Superset, fully Docker-Compose deployed
- **Fraud Ring Detection**: Kafka → Flink `windowAll` cycle detector → Redis pub/sub → Flask + vis.js live graph dashboard
- **Data Refinement Engine**: Streamlit app with per-field transform / validate / derive pipeline and AI-assisted function generation
- **Airflow DAG Builder**: React + React Flow drag-and-drop canvas with YAML export
- **Spark Simulator**: Streamlit app modelling Driver + Executor cluster with fault-tolerance and shuffle visualisation
