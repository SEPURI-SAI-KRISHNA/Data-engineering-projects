# Fraud Ring Detection

Detects money-laundering rings (A pays B pays C pays A) in a live transaction
stream. Transactions flow through Kafka into a Flink job that builds a payment
graph per time window and searches it for directed cycles. Alerts land in
Redis and a small Flask dashboard renders the rings as a live graph.

```
datagen.py -> Kafka -> Flink (CycleDetector) -> Redis pub/sub -> Flask + vis.js
```

## Running it

1. Start the infrastructure:

   ```
   docker compose up -d
   ```

2. Build and submit the Flink job (Flink UI is at http://localhost:8081):

   ```
   cd fraud-ring
   mvn package
   docker cp target/fraud-detector-1.0-SNAPSHOT.jar <jobmanager-container>:/job.jar
   docker exec <jobmanager-container> flink run -d /job.jar
   ```

3. Start the transaction generator. It produces mostly random noise and
   injects a ring of 3-5 accounts every few seconds:

   ```
   pip install kafka-python
   python datagen.py
   ```

4. Start the dashboard and open http://localhost:5000:

   ```
   cd dashboard
   pip install flask flask-socketio redis eventlet
   python app.py
   ```

## How detection works

`RingFinder` builds an adjacency map from each 5-second window of
transactions and runs a DFS from every account, tracking the current path.
Stepping onto an account already on the path means the slice from its first
occurrence to the top of the stack is a ring. Rings are deduplicated by
rotating each one so its smallest account comes first, so the same loop found
from different starting points is only reported once.

Alerts are JSON, one per window:

```json
{"windowStart": 1719000000000, "windowEnd": 1719000005000, "rings": [["User12", "User40", "User7"]]}
```

## Known limitations

- The window operator runs with parallelism 1 (`windowAll`) because cycle
  detection needs a global view of the graph. Scaling out would mean keying
  by a coarser partition (region, bank) and detecting per key.
- Windows use processing time, and a ring split across two windows is
  missed. Event-time windows with allowed lateness, or a keyed process
  function with state TTL, would close that gap.
- Two-account loops (A pays B, B refunds A) are reported too; a real system
  would score rings by amount similarity and timing instead of flagging
  every cycle.

Unit tests for the graph logic: `cd fraud-ring && mvn test`.
