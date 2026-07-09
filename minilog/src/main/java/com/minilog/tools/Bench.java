package com.minilog.tools;

import com.minilog.FsyncPolicy;
import com.minilog.Log;
import com.minilog.LogConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Measures what the ack contract costs. Each scenario appends into a fresh
 * log for a fixed wall-clock window and reports records/s, payload MB/s,
 * and the mean latency of one acked call. Also times recovery of a large
 * tail segment, since startup cost is part of the design (DESIGN.md).
 *
 * Results are machine- and filesystem-specific; run it yourself:
 *   mvn package -q && java -cp target/classes com.minilog.tools.Bench
 */
public final class Bench {

    private static final double WARMUP_SECONDS = 0.5;
    private static final double MEASURE_SECONDS = 3.0;

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args.length > 0 ? args[0] : "bench-data");
        deleteRecursively(root);
        Files.createDirectories(root);

        System.out.println("| scenario | payload | records/s | MB/s | mean ack latency |");
        System.out.println("|---|---|---|---|---|");

        appendBench(root, "fsync per append", FsyncPolicy.ALWAYS, 100, 1);
        appendBench(root, "fsync per append", FsyncPolicy.ALWAYS, 1024, 1);
        appendBench(root, "batch of 100, one fsync", FsyncPolicy.ALWAYS, 100, 100);
        appendBench(root, "batch of 100, one fsync", FsyncPolicy.ALWAYS, 1024, 100);
        appendBench(root, "no fsync (page cache only)", FsyncPolicy.OS, 100, 1);
        appendBench(root, "no fsync (page cache only)", FsyncPolicy.OS, 1024, 1);

        recoveryBench(root);
        deleteRecursively(root);
    }

    private static void appendBench(Path root, String name, FsyncPolicy policy,
                                    int payloadBytes, int batchSize) throws IOException {
        Path dir = root.resolve(name.replaceAll("\\W+", "-") + "-" + payloadBytes + "b");
        byte[] payload = new byte[payloadBytes];
        new Random(42).nextBytes(payload);
        List<byte[]> batch = Collections.nCopies(batchSize, payload);

        try (Log log = Log.open(dir, new LogConfig(64L * 1024 * 1024, 1024 * 1024, policy))) {
            runFor(log, batch, WARMUP_SECONDS);
            long calls = runFor(log, batch, MEASURE_SECONDS);

            long records = calls * batchSize;
            double perSec = records / MEASURE_SECONDS;
            double mbPerSec = perSec * payloadBytes / 1_000_000.0;
            double ackLatencyUs = MEASURE_SECONDS * 1_000_000.0 / calls;
            System.out.printf("| %s | %d B | %,.0f | %.1f | %s |%n",
                    name, payloadBytes, perSec, mbPerSec, formatLatency(ackLatencyUs));
        }
        deleteRecursively(dir);
    }

    private static long runFor(Log log, List<byte[]> batch, double seconds) throws IOException {
        long deadline = System.nanoTime() + (long) (seconds * 1e9);
        long calls = 0;
        while (System.nanoTime() < deadline) {
            if (batch.size() == 1) {
                log.append(batch.get(0));
            } else {
                log.appendBatch(batch);
            }
            calls++;
        }
        return calls;
    }

    private static void recoveryBench(Path root) throws IOException {
        Path dir = root.resolve("recovery");
        // one huge segment so the whole write becomes the tail that open() must scan
        LogConfig config = new LogConfig(1L << 30, 1024 * 1024, FsyncPolicy.OS);

        byte[] payload = new byte[1024];
        new Random(42).nextBytes(payload);
        List<byte[]> batch = Collections.nCopies(128, payload);
        long records = 128L * 1024;
        try (Log log = Log.open(dir, config)) {
            for (long written = 0; written < records; written += batch.size()) {
                log.appendBatch(batch);
            }
        }

        double tailMb = records * (1024 + 16) / 1_000_000.0;
        long start = System.nanoTime();
        try (Log log = Log.open(dir, config)) {
            if (log.nextOffset() != records) {
                throw new IllegalStateException("recovery lost records: " + log.nextOffset());
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf("%nRecovery scan of a %.0f MB tail segment (%,d records): %.2f s (%.0f MB/s)%n",
                tailMb, records, seconds, tailMb / seconds);
        deleteRecursively(dir);
    }

    private static String formatLatency(double us) {
        return us >= 1000 ? String.format("%.2f ms", us / 1000) : String.format("%.1f µs", us);
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
