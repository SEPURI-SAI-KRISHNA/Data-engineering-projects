package com.minilog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetentionTest {

    // ~4 records per segment at this size
    private static final LogConfig TINY = new LogConfig(64, 1024, FsyncPolicy.ALWAYS);

    @TempDir
    Path dir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private Log logWithRecords(int count) throws IOException {
        Log log = Log.open(dir, TINY);
        for (int i = 0; i < count; i++) {
            log.append(bytes("record-" + i));
        }
        return log;
    }

    private long segmentFileCount() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(".log")).count();
        }
    }

    @Test
    void deletesOnlyWholeSealedSegments() throws IOException {
        try (Log log = logWithRecords(30)) {
            long before = segmentFileCount();
            int deleted = log.deleteUpTo(15);

            assertTrue(deleted > 0);
            assertEquals(before - deleted, segmentFileCount());
            assertTrue(log.firstOffset() <= 15, "the segment containing offset 15 must survive");
            assertTrue(log.firstOffset() > 0);

            List<LogRecord> rest = log.readFrom(log.firstOffset(), 100);
            assertEquals(30 - log.firstOffset(), rest.size());
            assertEquals(log.firstOffset(), rest.get(0).offset());
        }
    }

    @Test
    void neverDeletesTheActiveSegment() throws IOException {
        try (Log log = logWithRecords(30)) {
            log.deleteUpTo(Long.MAX_VALUE);

            assertEquals(1, segmentFileCount());
            assertEquals(30, log.nextOffset(), "retention must not move the write position");
            assertEquals(30, log.append(bytes("still-writable")));
        }
    }

    @Test
    void readsBelowFirstOffsetAreRejected() throws IOException {
        try (Log log = logWithRecords(30)) {
            log.deleteUpTo(20);
            long first = log.firstOffset();

            assertThrows(IllegalArgumentException.class, () -> log.readFrom(first - 1, 10));
            assertEquals(first, log.readFrom(first, 1).get(0).offset());
        }
    }

    @Test
    void survivesReopenAfterRetention() throws IOException {
        long first;
        try (Log log = logWithRecords(30)) {
            log.deleteUpTo(15);
            first = log.firstOffset();
        }

        try (Log log = Log.open(dir, TINY)) {
            assertEquals(first, log.firstOffset());
            assertEquals(30, log.nextOffset());
            assertArrayEquals(bytes("record-29"), log.read(29).payload());
            assertEquals(30, log.append(bytes("after-reopen")));
        }
    }

    @Test
    void consumerClampsForwardWhenRetentionPassedItsCommit() throws IOException {
        try (Log log = logWithRecords(30)) {
            OffsetStore offsets = OffsetStore.open(dir.resolve("offsets"));
            offsets.commit("g", 2);

            log.deleteUpTo(20);
            Consumer consumer = Consumer.open(log, offsets, "g");

            assertEquals(log.firstOffset(), consumer.position(),
                    "committed offset 2 is gone; the consumer must jump to firstOffset");
            assertEquals(log.firstOffset(), consumer.poll(1).get(0).offset());
        }
    }
}
