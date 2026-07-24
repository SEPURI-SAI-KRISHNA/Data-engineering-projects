# Benchmarks

What the ack contract costs, measured. Run with:

```
mvn package -q
java -cp target/classes com.minilog.tools.Bench
```

Numbers below are from one machine and one run — treat the ratios as the
result, not the absolute values.

**Environment:** Intel i7-9750H (6c/12t), NVMe SSD, ext4, Linux 6.8,
OpenJDK 21. Single writer thread. Each scenario appends into a fresh log
for 3 seconds after a 0.5 s warmup.

| scenario | payload | records/s | MB/s | mean ack latency |
|---|---|---|---|---|
| fsync per append | 100 B | 398 | 0.0 | 2.51 ms |
| fsync per append | 1024 B | 407 | 0.4 | 2.46 ms |
| batch of 100, one fsync | 100 B | 21,200 | 2.1 | 4.72 ms |
| batch of 100, one fsync | 1024 B | 25,800 | 26.4 | 3.88 ms |
| no fsync (page cache only) | 100 B | 642,828 | 64.3 | 1.6 µs |
| no fsync (page cache only) | 1024 B | 88,758 | 90.9 | 11.3 µs |

Recovery scan of a 136 MB tail segment (131,072 records): 0.62 s (221 MB/s).

## Reading the table

- **fsync is the whole story.** Per-append throughput is ~400/s regardless
  of payload size, and the ~2.5 ms mean latency is simply this SSD's fsync
  time. The payload is noise next to the flush.
- **The contract costs ~1600x** (642k/s in page cache vs 400/s durable) if
  you insist on an fsync per record.
- **Batching buys back a factor of ~50** at the same durability: one fsync
  amortized over 100 records gets 21–26k records/s. A caller with 4 ms of
  latency budget per batch loses nothing and gains 50x throughput — this is
  why every serious log (Kafka, Postgres WAL, etcd) group-commits.
- **Payload size only matters once fsync stops dominating**: in page-cache
  mode 1 KiB records move ~91 MB/s vs ~64 MB/s for 100 B records, because
  per-record overhead (allocation, CRC, header) is amortized over more bytes.
- **Recovery scans at ~221 MB/s**, so even a worst-case 64 MB tail segment
  (the default size) costs ~0.3 s at startup. That's the price of the
  tail-only recovery design; scanning the whole log instead would multiply
  it by the segment count.

## Caveats

- `MB/s` counts payload bytes only, not the 16-byte headers.
- One JVM, one run per scenario, wall-clock timeboxed; good enough for
  order-of-magnitude ratios, not for publication-grade statistics.
- Latency is the *mean* (derived from throughput). Tail latencies would
  need a histogram; that's future work if it starts mattering.
- Page-cache mode numbers depend on memory pressure; on a loaded box the
  kernel will throttle writeback.
