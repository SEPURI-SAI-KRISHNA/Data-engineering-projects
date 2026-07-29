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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogTest {

    @TempDir
    Path dir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void appendAssignsSequentialOffsets() throws IOException {
        try (Log log = Log.open(dir, LogConfig.defaults())) {
            assertEquals(0, log.append(bytes("a")));
            assertEquals(1, log.append(bytes("b")));
            assertEquals(2, log.append(bytes("c")));
            assertEquals(3, log.nextOffset());
        }
    }

    @Test
    void readsBackWhatWasWritten() throws IOException {
        try (Log log = Log.open(dir, LogConfig.defaults())) {
            log.append(bytes("hello"));
            log.append(bytes("world"));

            assertArrayEquals(bytes("hello"), log.read(0).payload());
            assertArrayEquals(bytes("world"), log.read(1).payload());
            assertNull(log.read(2));
        }
    }

    @Test
    void rollsIntoNewSegmentFiles() throws IOException {
        LogConfig tiny = new LogConfig(64, 1024, FsyncPolicy.ALWAYS);
        try (Log log = Log.open(dir, tiny)) {
            for (int i = 0; i < 20; i++) {
                log.append(bytes("record-" + i));
            }
        }
        try (Stream<Path> files = Files.list(dir)) {
            assertTrue(files.filter(p -> p.toString().endsWith(".log")).count() > 1,
                    "expected the log to roll into multiple segment files");
        }
    }

    @Test
    void readsSpanSegmentBoundaries() throws IOException {
        LogConfig tiny = new LogConfig(64, 1024, FsyncPolicy.ALWAYS);
        try (Log log = Log.open(dir, tiny)) {
            for (int i = 0; i < 20; i++) {
                log.append(bytes("record-" + i));
            }

            List<LogRecord> all = log.readFrom(0, 100);
            assertEquals(20, all.size());
            for (int i = 0; i < 20; i++) {
                assertEquals(i, all.get(i).offset());
                assertArrayEquals(bytes("record-" + i), all.get(i).payload());
            }

            List<LogRecord> tail = log.readFrom(15, 100);
            assertEquals(5, tail.size());
            assertEquals(15, tail.get(0).offset());
        }
    }

    @Test
    void appendBatchIsContiguousAndReadable() throws IOException {
        try (Log log = Log.open(dir, LogConfig.defaults())) {
            log.append(bytes("solo"));
            long first = log.appendBatch(List.of(bytes("b0"), bytes("b1"), bytes("b2")));

            assertEquals(1, first);
            assertEquals(4, log.nextOffset());
            assertArrayEquals(bytes("b2"), log.read(3).payload());
        }
    }

    @Test
    void appendBatchSurvivesASegmentRoll() throws IOException {
        LogConfig tiny = new LogConfig(64, 1024, FsyncPolicy.ALWAYS);
        try (Log log = Log.open(dir, tiny)) {
            List<byte[]> batch = new java.util.ArrayList<>();
            for (int i = 0; i < 20; i++) {
                batch.add(bytes("batch-record-" + i));
            }
            log.appendBatch(batch);
            assertEquals(20, log.readFrom(0, 100).size());
        }
        // and everything is still there after recovery
        try (Log log = Log.open(dir, tiny)) {
            assertEquals(20, log.nextOffset());
        }
    }

    @Test
    void appendBatchRejectsBadBatchesWithoutWritingAnything() throws IOException {
        LogConfig config = new LogConfig(1024 * 1024, 10, FsyncPolicy.ALWAYS);
        try (Log log = Log.open(dir, config)) {
            assertThrows(IllegalArgumentException.class, () -> log.appendBatch(List.of()));
            assertThrows(IllegalArgumentException.class,
                    () -> log.appendBatch(List.of(bytes("ok"), new byte[11])));
            assertEquals(0, log.nextOffset(), "a rejected batch must not consume offsets");
        }
    }

    @Test
    void randomAccessReadsAreCorrectOnALargeSegment() throws IOException {
        // enough records to give the sparse index many intervals to cover
        try (Log log = Log.open(dir, new LogConfig(16 * 1024 * 1024, 1024, FsyncPolicy.OS))) {
            for (int i = 0; i < 5000; i++) {
                log.append(bytes("record-" + i));
            }
            for (long offset : new long[]{0, 1, 499, 2500, 4321, 4999}) {
                assertArrayEquals(bytes("record-" + offset), log.read(offset).payload());
            }
            List<LogRecord> batch = log.readFrom(4990, 100);
            assertEquals(10, batch.size());
            assertEquals(4990, batch.get(0).offset());
        }
    }

    @Test
    void rejectsOversizedRecords() throws IOException {
        LogConfig config = new LogConfig(1024 * 1024, 10, FsyncPolicy.ALWAYS);
        try (Log log = Log.open(dir, config)) {
            assertThrows(IllegalArgumentException.class, () -> log.append(new byte[11]));
            // the rejection must not have consumed an offset
            assertEquals(0, log.append(new byte[10]));
        }
    }
}
