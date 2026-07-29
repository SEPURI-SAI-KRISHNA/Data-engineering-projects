# Streaming Lakehouse Platform

An end-to-end, locally deployed data platform: event streaming through Kafka, stateful stream processing in Flink, Iceberg tables on MinIO, and federated SQL through Trino.

## 🏗️ Architecture
Docker Compose orchestrates the stack:

* **Ingestion:** Python generator simulating IoT telemetry, using `msgspec` for fast JSON serialization.
* **Message Broker:** Apache Kafka (3 partitions) routing ordered events via Murmur2 key hashing.
* **Stream Processing:** Apache Flink executing stateful tumbling window aggregations with exactly-once semantics and checkpointing.
* **Storage Layer:** MinIO (S3-compatible) storing Apache Iceberg parquet files.
* **Metadata Catalog:** Project Nessie managing Iceberg table transactions.
* **Query Engine:** Trino providing federated SQL access over the data lake.
* **Visualization:** Apache Superset serving real-time, auto-refreshing dashboards.

## ⚙️ Design Notes
* **Serialization:** `msgspec.Struct` instead of the stdlib `json` module for cheaper encoding on the producer side.
* **Partitioning:** Kafka messages are keyed on `sensor_id`, so each sensor's events stay ordered within a partition and downstream state stays correct.
* **Stateful Windows:** Flink SQL `TUMBLE` windows with event-time watermarking handle late-arriving data and compute rolling 10-second temperature averages.
* **Decoupled Compute/Storage:** Flink/Trino compute is fully separated from the MinIO storage layer, mirroring production setups.

## 🚀 Quick Start
**0. Fetch the Flink connector jars** (too big for git)
```bash
./processing/flink/lib/download-jars.sh
```

**1. Boot the Infrastructure**
```bash
docker compose up -d
```

**2. Initialize the Lakehouse Schema (Trino)**
```bash
docker exec -it trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.telemetry WITH (location = 's3a://warehouse/');"
```

**3. Start the Flink Processing Job**
Submit the SQL job located in the documentation to the Flink JobManager to begin checkpointing and sinking to Iceberg.

**4. Run the Ingestion Engine**
```bash
python ingestion/producer.py
```

**5. View Live Data**
Access Superset at `http://localhost:8088` (admin/admin), connect to Trino via `trino://admin@trino:8080/iceberg`, and build your real-time dashboard.
