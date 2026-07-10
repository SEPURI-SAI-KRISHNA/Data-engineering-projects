# minilog — design

A single-node durable log with an honest ack contract: **if `append()`
returned, the record survives `kill -9`.** Everything in this document exists
to make that one sentence true, provable, and cheap enough to be useful.

This is a learning-in-public systems project. The interesting output is not
the code — Kafka exists — it's the recovery reasoning, the failure-mode
analysis, and the test harness that tries to falsify the ack contract
thousands of times.

## Goals

- Append/read a durable, offset-addressed record log on one machine.
- A precise ack contract, stated in terms of `fsync`, and verified by a
  crash harness (`chaos/chaos_run.py`) that SIGKILLs the writer mid-flight.
- Recovery that never invents data: after a crash the log contains exactly
  a prefix of what was written, ending at or after the last acked record.
- Startup cost proportional to one segment, not the whole log.
- Measured numbers (phase 2): throughput vs fsync policy, recovery time vs
  tail size.

## Non-goals (and why)

- **Replication / consensus.** Single-node durability done honestly is a
  complete project. Replication without a real consensus protocol is
  theater; with one, it's a different project.
- **Network protocol.** minilog is an embedded library plus CLI tools.
  A wire protocol adds surface area without adding durability insight.
- **Multi-writer concurrency.** One writer, `synchronized`. The single-writer
  design is what makes recovery reasoning airtight; lock contention is not
  the problem being studied.
- **Background scrubbing / bit-rot repair.** Sealed segments are CRC-checked
  on read and corruption fails loudly, but there is no proactive scan and no
  redundancy to repair from.

## On-disk format

A log is a directory of segment files named by the offset of their first
record, `%020d.log`. Within a segment, records are laid out back to back:

```
offset   8 bytes   logical offset of this record
length   4 bytes   payload size in bytes
crc      4 bytes   CRC32 over offset + length + payload
payload  n bytes
```

Two deliberate redundancies:

- **The offset is stored** even though it is derivable from scan position.
  Recovery cross-checks it against the expected next offset, which catches
  file splicing, truncation-in-the-middle, and misaligned scans for the
  price of 8 bytes per record.
- **The CRC covers the header fields too**, not just the payload, so a
  corrupted length can't send the scanner off into the weeds undetected —
  it either fails the bounds check or fails the CRC.

## Write path and the ack contract

```
append(payload):
  roll segment if the active one is full
  write record at end of active segment
  if fsync policy is ALWAYS: FileChannel.force()
  return offset            <- this is the ack
```

With `FsyncPolicy.ALWAYS`, the ack means the record's bytes were flushed to
the device before `append()` returned. With `FsyncPolicy.OS`, acks only mean
"in the page cache" and a crash can lose a suffix — that mode exists for
benchmarking the cost of the contract, and the difference IS the benchmark.

**Batching: `appendBatch(payloads)`** writes every record, fsyncs once, then
returns. The contract is unchanged — return still means everything in the
batch is durable — but the fsync cost is amortized over the batch. This is
group commit with the grouping made explicit at the API instead of implicit
across threads. The classic alternative (concurrent appenders sharing one
fsync, Postgres-style) was rejected because multi-writer concurrency is a
non-goal; the batch API buys the same amortization with none of the
concurrency risk, and the benchmarks quantify exactly what it buys.
A batch may straddle a segment roll; that's fine — the roll fsyncs the old
segment, the final fsync covers the new one, so everything acked is durable.
All payloads are size-validated before any is written, so a rejected batch
writes nothing.

**Rolling keeps an invariant: only the last segment can ever be torn.**
The sequence is: fsync the old segment, create the new file, fsync the
directory, only then append to it. Two subtle points:

- The directory fsync matters. A file's existence is directory metadata; a
  crash after creating the segment but before the directory entry is durable
  would silently lose the whole new segment. Acking a record in a segment
  whose directory entry isn't durable would break the contract, so the
  directory is forced before the first append.
- If `fsync` itself fails, the log marks itself failed and refuses further
  appends. After a failed fsync the page cache state is undefined — you
  cannot retry the fsync and trust success (the PostgreSQL "fsyncgate"
  lesson, 2018). The only honest move is to stop.

## Recovery

On open, sealed segments are trusted (see the rolling invariant) and only
the tail segment is scanned:

```
pos = 0, expected = segment base offset
loop:
  fewer than 16 header bytes left           -> stop (torn header)
  stored offset != expected                 -> stop (misaligned/spliced)
  length < 0 or > max or past end of file   -> stop (torn/corrupt length)
  CRC mismatch                              -> stop (torn/corrupt payload)
  pos += 16 + length, expected += 1
truncate file at pos
```

Truncation discards only records that were never acked (an acked record was
fsynced, and fsync completes before anything after it is written). Recovery
therefore restores exactly the acked prefix, possibly plus a few complete
unacked records that made it to disk — which is allowed by the contract.

## Consumers and committed offsets

A `Consumer` is a cursor over the log with a durable checkpoint. `poll()`
advances an in-memory position; `commit()` makes it durable. The delivery
guarantee is **at-least-once**: records polled after the last commit are
re-delivered if the process dies before committing. Committing *before*
processing would flip that to at-most-once (records can be lost, never
repeated) — the library picks neither; the caller chooses by where they put
`commit()`. Exactly-once is not a commit-placement trick, it needs the
downstream store to participate (phase 3).

Committed offsets are stored in one small checkpoint file per group
(`offsets/<group>.ckpt`), written with the atomic-rename protocol:

```
write offset + CRC to <group>.tmp
fsync the tmp file
rename tmp over <group>.ckpt      <- atomic on POSIX
fsync the directory
```

A crash leaves either the old checkpoint or the new one, never a torn file.
If a checkpoint is missing or fails its CRC anyway, the consumer restarts
from offset 0 — with at-least-once semantics, falling backwards is the safe
direction; falling forwards (guessing a higher offset) would silently skip
records.

**Why not store offsets in the log itself?** Kafka does (the
`__consumer_offsets` topic), but that design drags in log compaction, which
minilog doesn't have yet. Offsets are a few bytes of state per group;
an atomic-rename checkpoint file is the correctly-sized tool. Revisit if
compaction lands for other reasons.

## Exactly-once, honestly

"Exactly-once delivery" is a marketing phrase — deliveries can always
repeat. What is achievable is exactly-once *processing*: every record's
effect appears in the output exactly once, across any number of crashes.
The requirement fits in one sentence: **the consumer's position and the
effects of its records must commit atomically.** Persist them separately
and a crash between the two commits either replays records into state that
already contains them (over-count) or advances past records the state never
saw (loss). No commit *ordering* fixes this; only atomicity does.

`SnapshotStore` provides that atomicity for state that fits in a file:
`save(offset, state)` writes both into one file using the same
tmp → fsync → rename → dir-fsync protocol as offset checkpoints. A restart
loads the pair; disagreeing is structurally impossible. A missing or
corrupt snapshot degrades to reprocessing from the start with *empty*
state — wasteful but still exact, because state and position reset
together.

The pattern generalizes beyond files: with a database as the sink, the
offset lives in a table updated in the same transaction as the output —
which is what Flink's two-phase sinks and Kafka Connect's offset topics
arrange with more machinery. Bundling SQLite to demo that was considered
and rejected: a dependency would hide the mechanism this project exists to
expose, and "put the offset in the same transaction" transfers directly.

`chaos/chaos_exactly_once.py` proves both directions. It crash-loops a
counting pipeline (`ChaosCounter`) until it finishes and requires the final
counts to be bit-exact against a recount of the log. Then it runs the same
pipeline in naive mode — state flushed every 500 records, offsets committed
every 1000, the mismatched-cadence setup of many real consumers with
auto-commit on — and shows the counts coming out wrong.

## Retention

`deleteUpTo(offset)` removes sealed segments whose entire range lies below
the given offset. The active segment is never deleted, so the log is never
empty and the append path never changes. Deletes run oldest-first with a
directory fsync after each one — a barrier per file — so a crash
mid-retention can only leave a contiguous suffix of segments, never a gap.

`firstOffset()` exposes the log's new beginning. Reads below it are
rejected loudly rather than returning silently-empty results — a consumer
that fell behind retention should find out, not spin. `Consumer.open`
clamps its resume position forward to `firstOffset()`: the records are
gone, so jumping forward (Kafka's `auto.offset.reset=earliest`) is the only
honest option left.

## Sparse index

Reads originally scanned a segment from the top on every call — correct,
but O(segment) per poll, which makes a consumer loop O(segment²). Each
segment now keeps a small in-memory index: (offset, file position) roughly
every 4 KB of log, built lazily on first read and extended as records are
appended. Reads binary-search the index and scan at most ~one interval of
file. The index is deliberately not persisted: rebuilding it is one
sequential header scan on first read per segment, and a persisted index
would be a second on-disk structure that recovery would have to distrust
and re-verify anyway. It's a pure performance layer — nothing about
correctness or durability depends on it.

## Failure modes

| failure | what's on disk | detected by | outcome |
|---|---|---|---|
| kill -9 between write and fsync | complete but unacked tail records | nothing to detect — records are valid | records survive or are truncated; both fine, they were never acked |
| torn write (partial record at tail) | half a header or short payload | header-bytes / bounds check | tail truncated at last valid record |
| torn write (garbage in tail record) | full-length record, wrong bytes | CRC mismatch | tail truncated at last valid record |
| corrupted length field at tail | absurd or out-of-bounds length | bounds check (0..maxRecordBytes, within file) | tail truncated |
| crash during roll, before dir entry durable | new segment file missing | prevented: dir fsync before first append | previous segment is the tail; nothing acked was in the new file |
| fsync returns an error | unknown — page cache state undefined | IOException from `force()` | log poisoned, all further appends refused |
| disk full | write fails partway | IOException from `write()` | append not acked; next open truncates the partial record |
| bit rot in a sealed segment | valid-looking file, bad bytes | CRC check on read | read throws; no repair (non-goal) |
| crash between checkpoint tmp write and rename | old ckpt intact, stray .tmp | rename never happened | old committed offset stands; some records re-delivered (allowed by at-least-once) |
| corrupt or missing offset checkpoint | bad CRC / no file | CRC + length check on read | consumer restarts from 0; re-delivery, never skipping |
| crash mid-retention | oldest few segments gone | oldest-first deletes with a dir fsync each | survivors are a contiguous suffix; firstOffset just moved less far than asked |
| naive sink: crash between state flush and offset commit | state and offset disagree | demonstrated, not prevented: `chaos_exactly_once.py` naive mode | over-count on resume — the bug SnapshotStore exists to make impossible |

## Alternatives considered

- **mmap vs FileChannel.** FileChannel. Explicit `write` + `force` makes the
  ordering argument checkable by reading the code; `msync` semantics and
  page-fault-driven writes make the same argument murkier. mmap wins on read
  throughput, which is not the contract under study.
- **CRC per record vs per block/batch.** Per record. Simpler recovery loop,
  ~16 bytes overhead per record is acceptable at this scale. Kafka amortizes
  CRCs over record batches; revisit if/when batching lands in phase 2.
- **Separate index file vs scan-to-read.** Phase 1 scanned within a segment;
  phase 2 added a lazy in-memory sparse index (see its section above). A
  *persisted* index file remains rejected: it would be a second on-disk
  structure recovery has to distrust, for a rebuild cost of one sequential
  scan.
- **Scanning every segment at startup vs tail only.** Tail only, O(one
  segment) startup. The price is the rolling invariant (fsync-before-roll),
  which costs one extra fsync per segment — negligible.

## Test strategy

- **Unit tests** (`mvn test`): append/read round-trips, segment rolling,
  reads across segments, oversized-record rejection.
- **Recovery tests**: corruption is injected deterministically — truncated
  headers, short payloads, flipped payload bits, absurd lengths written
  straight into the segment file — then the log is reopened and must
  truncate to exactly the last valid record and keep working.
- **Chaos harness** (`chaos/chaos_run.py`): repeatedly SIGKILLs a writer JVM
  at random moments, restarts, and verifies that every acked offset is
  present after recovery with exactly the expected payload, and that offsets
  are gapless. Each round reuses the same log dir, so recovery itself is
  exercised every round.

**What the harness does and doesn't prove.** `kill -9` destroys the process
but not the OS page cache — the kernel still flushes written-but-unfsynced
data eventually, so even `FsyncPolicy.OS` usually survives a process crash.
The harness therefore proves *recovery correctness* (torn records, mid-roll
crashes, the scan-and-truncate logic, offset continuity), not power-loss
durability. Testing the fsync policy itself means yanking the storage stack
out from under the filesystem: dm-flakey, a VM with virtual power cuts, or
real hardware. That's a roadmap item; claiming more than the harness shows
would be exactly the kind of dishonesty this project exists to avoid.

## Roadmap

1. **(done)** Log core, recovery, unit + recovery tests, chaos harness.
2. **(done)** Consumer groups with committed offsets (atomic-rename
   checkpoints), batch append / group commit, sparse index, benchmark
   suite — see BENCHMARKS.md for measured numbers.
3. **(done)** Exactly-once processing via atomic state+offset snapshots,
   with a chaos proof that the naive split over-counts; retention with
   crash-safe oldest-first deletion.
4. Power-loss durability testing via dm-flakey or a crash-injecting VM,
   to verify the fsync policy itself rather than just recovery logic.
