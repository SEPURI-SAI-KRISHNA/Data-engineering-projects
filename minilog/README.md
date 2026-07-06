# minilog

A single-node durable log with an honest ack contract: **if `append()`
returned, the record survives `kill -9`.** Built from scratch to understand
what systems like Kafka actually promise and what it costs to keep the
promise. The design rationale, failure-mode table, and the limits of what
the tests prove are in [DESIGN.md](DESIGN.md) — read that first, it's the
real deliverable.

```java
try (Log log = Log.open(Path.of("/data/mylog"), LogConfig.defaults())) {
    long offset = log.append("hello".getBytes());   // fsynced before this returns
    log.appendBatch(payloads);                      // same contract, one fsync for all

    OffsetStore offsets = OffsetStore.open(Path.of("/data/offsets"));
    Consumer consumer = Consumer.open(log, offsets, "billing");
    for (LogRecord record : consumer.poll(100)) {
        process(record);                            // at-least-once: commit after processing
    }
    consumer.commit();                              // durable when this returns
}
```

## Verifying the claim

Unit and recovery tests (torn headers, short payloads, flipped bits,
absurd lengths, mid-roll crashes):

```
mvn test
```

The chaos harness SIGKILLs a writer JVM at random moments in a loop and
checks after each crash that every acked record survived recovery intact
and offsets are gapless:

```
mvn package -q
python3 chaos/chaos_run.py 100
```

A 25-round run looks like this:

```
round 1: killed writer, 352 acks this round, log recovered to offset 353, all 352 acked records intact
round 2: killed writer, 88 acks this round, log recovered to offset 442, all 440 acked records intact
...
round 25: killed writer, 321 acks this round, log recovered to offset 6094, all 6069 acked records intact

25 kill -9 crashes, 6069 acked records, zero lost, zero corrupt.
```

(The small gap between "recovered to offset" and "acked records" is
expected: an ACK line can die in the killed process's pipe buffer. That's
the safe direction — the record is on disk, we just didn't hear about it.)

Honesty note: `kill -9` does not clear the OS page cache, so this proves
recovery correctness, not power-loss durability. See "What the harness does
and doesn't prove" in DESIGN.md.

## Exactly-once, demonstrated both ways

Exactly-once processing has one requirement: the consumer's position and
the effects of its records must commit **atomically** (`SnapshotStore`
saves both in one atomically-renamed file). The second chaos harness proves
the requirement is real by crash-looping a counting pipeline in both the
correct and the naive configuration:

```
$ python3 chaos/chaos_exactly_once.py

-- atomic snapshot (state + offset committed together) --
46 crashes -> EXACT total=100000

-- naive split (state and offset on separate cadences) --
38 crashes -> MISMATCH expected=100000 actual=108500 (off by 8500)
```

The naive mode is not a strawman: state flushed on one cadence and offsets
auto-committed on another is the default shape of many real consumers. The
8,500 extra counts are records that were replayed into state that already
included them. See "Exactly-once, honestly" in DESIGN.md.

## What the contract costs

Measured on this machine (see [BENCHMARKS.md](BENCHMARKS.md) for the full
table and caveats): an fsync per append caps at ~400 records/s because
each ack pays ~2.5 ms of flush; batching 100 records behind one fsync gets
21–26k records/s at identical durability; skipping fsync entirely runs
~1600x faster and is exactly as durable as your luck. Recovery scans a
crashed tail segment at ~221 MB/s.

```
mvn package -q
java -cp target/classes com.minilog.tools.Bench
```

## Layout

- `src/main/java/com/minilog/` — `Log`, `LogSegment`, `Consumer`, `OffsetStore`, `SnapshotStore`
- `src/main/java/com/minilog/tools/` — `ChaosWriter`/`LogDump`, `ChaosCounter` (exactly-once demo), `Bench`
- `src/test/java/` — unit + deterministic corruption/recovery tests
- `chaos/chaos_run.py` — the kill -9 durability harness
- `chaos/chaos_exactly_once.py` — the exactly-once vs naive-split harness
- `DESIGN.md` — goals, non-goals, format, recovery reasoning, failure modes, roadmap
- `BENCHMARKS.md` — measured numbers and how to read them

## Status

Phases 1–3 are done: log core with crash-verified recovery; consumers with
durable committed offsets, group commit, sparse index, and benchmarks;
exactly-once processing via atomic state+offset snapshots (with the naive
counter-example demonstrated, not just asserted) and crash-safe retention.
Remaining: power-loss testing with dm-flakey. Roadmap at the end of
DESIGN.md.
