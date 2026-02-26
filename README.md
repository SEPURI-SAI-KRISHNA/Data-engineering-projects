# Ultra-Low Latency Streaming & Lakehouse Platform

An end-to-end, locally deployed distributed data platform demonstrating high-throughput event streaming, stateful stream processing, and open-table federated querying. 

## 🏗️ Architecture
This project implements a modern streaming lakehouse architecture using Docker Compose to orchestrate the following ecosystem:

* **Ingestion:** Custom Python generator simulating high-frequency IoT telemetry, utilizing `msgspec` for ultra-fast C-based JSON serialization.
* **Message Broker:** Apache Kafka (3 partitions) routing ordered events via Murmur2 key hashing.
* **Stream Processing:** Apache Flink executing stateful tumbling window aggregations with exactly-once semantics and checkpointing.
* **Storage Layer:** MinIO (S3-compatible) storing Apache Iceberg parquet files.
* **Metadata Catalog:** Project Nessie managing Iceberg table transactions.
* **Query Engine:** Trino providing federated SQL access over the data lake.
* **Visualization:** Apache Superset serving real-time, auto-refreshing dashboards.

## ⚙️ Low-Level Design (LLD) Highlights
* **Serialization Optimization:** Replaced standard Python `json` with `msgspec.Struct`, bypassing traditional serialization bottlenecks to maximize producer throughput.
* **Partition Strategies:** Enforced sensor-level ordering by keying Kafka messages on `sensor_id`, ensuring accurate state calculation downstream.
* **Stateful Windows:** Utilized Flink SQL `TUMBLE` windows with event-time watermarking to handle late-arriving data and calculate rolling 10-second temperature averages.
* **Decoupled Compute/Storage:** Completely separated the Flink/Trino compute engines from the MinIO storage layer, mimicking production Kubernetes environments.

## 🚀 Quick Start
**1. Boot the Infrastructure**
\`\`\`bash
docker compose up -d
\`\`\`

**2. Initialize the Lakehouse Schema (Trino)**
\`\`\`bash
docker exec -it trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.telemetry WITH (location = 's3a://warehouse/');"
\`\`\`

**3. Start the Flink Processing Job**
Submit the SQL job located in the documentation to the Flink JobManager to begin checkpointing and sinking to Iceberg.

**4. Run the Ingestion Engine**
\`\`\`bash
python ingestion/producer.py
\`\`\`

**5. View Live Data**
Access Superset at `http://localhost:8088` (admin/admin), connect to Trino via `trino://admin@trino:8080/iceberg`, and build your real-time dashboard. 