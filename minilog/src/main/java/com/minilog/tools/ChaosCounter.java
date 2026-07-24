package com.minilog.tools;

import com.minilog.FsyncPolicy;
import com.minilog.Log;
import com.minilog.LogConfig;
import com.minilog.LogRecord;
import com.minilog.OffsetStore;
import com.minilog.SnapshotStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * The exactly-once demonstration: counts records per key while the chaos
 * harness kills it. Two persistence strategies, one correct:
 *
 *   run          state and offset saved together in one atomic snapshot
 *   run --naive  state flushed every 500 records, offset committed every
 *                1000 -- the mismatched-cadence setup of a typical consumer
 *                with auto-commit on. A crash between the two leaves them
 *                disagreeing, and the final counts come out wrong.
 *
 * verify recounts the log from the start and compares against the sink's
 * state; exits 0 only on an exact match.
 */
public final class ChaosCounter {

    static final LogConfig CONFIG = new LogConfig(256 * 1024, 1024 * 1024, FsyncPolicy.OS);
    static final int KEYS = 10;
    static final int POLL_BATCH = 100;
    static final int STATE_EVERY = 500;
    static final int OFFSET_EVERY = 1000;
    static final String GROUP = "naive-counter";

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "build" -> build(Path.of(args[1]), Long.parseLong(args[2]));
            case "run" -> {
                boolean naive = args.length > 3 && args[3].equals("--naive");
                run(Path.of(args[1]), Path.of(args[2]), naive ? Path.of(args[4]) : null);
            }
            case "verify" -> verify(Path.of(args[1]), Path.of(args[2]));
            default -> throw new IllegalArgumentException("unknown command " + args[0]);
        }
    }

    static byte[] keyFor(long offset) {
        return ("k" + (offset % KEYS)).getBytes(StandardCharsets.UTF_8);
    }

    private static void build(Path logDir, long records) throws IOException {
        try (Log log = Log.open(logDir, CONFIG)) {
            List<byte[]> batch = new ArrayList<>();
            for (long offset = log.nextOffset(); offset < records; offset++) {
                batch.add(keyFor(offset));
                if (batch.size() == 1000) {
                    log.appendBatch(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                log.appendBatch(batch);
            }
        }
        System.out.println("built " + records + " records");
    }

    private static void run(Path logDir, Path snapshotFile, Path naiveOffsetsDir) throws Exception {
        try (Log log = Log.open(logDir, CONFIG)) {
            SnapshotStore snapshots = SnapshotStore.open(snapshotFile);
            OffsetStore offsets = naiveOffsetsDir == null ? null : OffsetStore.open(naiveOffsetsDir);

            TreeMap<String, Long> counts = new TreeMap<>();
            long position = log.firstOffset();

            var snap = snapshots.load();
            if (snap.isPresent()) {
                counts = deserialize(snap.get().state());
                position = snap.get().offset();
            }
            if (offsets != null) {
                // the naive bug: position comes from a file the state
                // knows nothing about
                position = offsets.committed(GROUP).orElse(log.firstOffset());
            }

            long sinceState = 0;
            long sinceOffset = 0;
            while (true) {
                List<LogRecord> batch = log.readFrom(position, POLL_BATCH);
                if (batch.isEmpty()) {
                    break;
                }
                for (LogRecord record : batch) {
                    counts.merge(new String(record.payload(), StandardCharsets.UTF_8), 1L, Long::sum);
                }
                position = batch.get(batch.size() - 1).offset() + 1;
                sinceState += batch.size();
                sinceOffset += batch.size();

                if (sinceState >= STATE_EVERY) {
                    snapshots.save(offsets == null ? position : 0, serialize(counts));
                    sinceState = 0;
                }
                if (offsets != null && sinceOffset >= OFFSET_EVERY) {
                    offsets.commit(GROUP, position);
                    sinceOffset = 0;
                }
                Thread.sleep(2); // pace it so the harness can land kills mid-run
            }

            snapshots.save(offsets == null ? position : 0, serialize(counts));
            if (offsets != null) {
                offsets.commit(GROUP, position);
            }
            System.out.println("DONE " + position);
        }
    }

    private static void verify(Path logDir, Path snapshotFile) throws IOException {
        TreeMap<String, Long> expected = new TreeMap<>();
        try (Log log = Log.open(logDir, CONFIG)) {
            long position = log.firstOffset();
            while (true) {
                List<LogRecord> batch = log.readFrom(position, 1000);
                if (batch.isEmpty()) {
                    break;
                }
                for (LogRecord record : batch) {
                    expected.merge(new String(record.payload(), StandardCharsets.UTF_8), 1L, Long::sum);
                }
                position = batch.get(batch.size() - 1).offset() + 1;
            }
        }

        TreeMap<String, Long> actual = SnapshotStore.open(snapshotFile).load()
                .map(s -> deserialize(s.state()))
                .orElseGet(TreeMap::new);

        long expectedTotal = expected.values().stream().mapToLong(Long::longValue).sum();
        long actualTotal = actual.values().stream().mapToLong(Long::longValue).sum();

        if (expected.equals(actual)) {
            System.out.println("EXACT total=" + actualTotal);
        } else {
            System.out.println("MISMATCH expected=" + expectedTotal + " actual=" + actualTotal
                    + " (off by " + (actualTotal - expectedTotal) + ")");
            System.exit(1);
        }
    }

    private static byte[] serialize(TreeMap<String, Long> counts) {
        StringBuilder sb = new StringBuilder();
        counts.forEach((key, count) -> sb.append(key).append('=').append(count).append('\n'));
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static TreeMap<String, Long> deserialize(byte[] state) {
        TreeMap<String, Long> counts = new TreeMap<>();
        String text = new String(state, StandardCharsets.UTF_8);
        for (String line : text.split("\n")) {
            if (!line.isEmpty()) {
                int eq = line.indexOf('=');
                counts.put(line.substring(0, eq), Long.parseLong(line.substring(eq + 1)));
            }
        }
        return counts;
    }
}
