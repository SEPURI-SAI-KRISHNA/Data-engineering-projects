package com.minilog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Simulates the on-disk aftermath of crashes by corrupting segment files
 * directly, then checks that reopening truncates to exactly the last valid
 * record and the log keeps working. The corruption cases mirror the failure
 * table in DESIGN.md.
 */
class RecoveryTest {

    @TempDir
    Path dir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private void writeRecords(int count) throws IOException {
        try (Log log = Log.open(dir, LogConfig.defaults())) {
            for (int i = 0; i < count; i++) {
                log.append(bytes("record-" + i));
            }
        }
    }

    private Path tailSegment() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(".log"))
                    .sorted()
                    .reduce((a, b) -> b)
                    .orElseThrow();
        }
    }

    private void appendRawBytes(byte[] junk) throws IOException {
        try (FileChannel ch = FileChannel.open(tailSegment(), StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ch.write(ByteBuffer.wrap(junk));
        }
    }

    @Test
    void survivesCleanReopen() throws IOException {
        writeRecords(50);
        try (Log log = Log.open(dir, LogConfig.defaults())) {
            assertEquals(50, log.nextOffset());
            assertArrayEquals(bytes("record-49"), log.read(49).payload());
        }
    }

    @Test
    void truncatesHalfWrittenHeader() throws IOException {
        writeRecords(10);
        appendRawBytes(new byte[]{1, 2, 3, 4, 5, 6, 7}); // 7 of 16 header bytes

        try (Log log = Log.open(dir, LogConfig.defaults())) {
            assertEquals(10, log.nextOffset());
            assertEquals(10, log.append(bytes("after-crash")));
            assertArrayEquals(bytes("after-crash"), log.read(10).payload());
        }
    }

    @Test
    void truncatesRecordWithMissingPayload() throws IOException {
        writeRecords(10);
        // a header claiming 100 payload bytes, but only 5 made it to disk
        ByteBuffer torn = ByteBuffer.allocate(LogSegment.HEADER_BYTES + 5);
        torn.putLong(10);
        torn.putInt(100);
        torn.putInt(0xDEAD);
        torn.put(new byte[]{1, 2, 3, 4, 5});
        appendRawBytes(torn.array());

        try (Log log = Log.open(dir, LogConfig.defaults())) {
            assertEquals(10, log.nextOffset());
        }
    }

    @Test
    void truncatesRecordWithCorruptPayload() throws IOException {
        writeRecords(10);
        Path tail = tailSegment();
        // flip one bit in the last record's payload
        long size = Files.size(tail);
        try (FileChannel ch = FileChannel.open(tail, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer b = ByteBuffer.allocate(1);
            ch.read(b, size - 1);
            b.flip();
            ByteBuffer flipped = ByteBuffer.wrap(new byte[]{(byte) (b.get() ^ 0x01)});
            ch.write(flipped, size - 1);
        }

        try (Log log = Log.open(dir, LogConfig.defaults())) {
            assertEquals(9, log.nextOffset(), "the corrupted last record should be dropped");
            assertArrayEquals(bytes("record-8"), log.read(8).payload());
            assertEquals(9, log.append(bytes("replacement")));
        }
    }

    @Test
    void truncatesRecordWithAbsurdLength() throws IOException {
        writeRecords(10);
        ByteBuffer bogus = ByteBuffer.allocate(LogSegment.HEADER_BYTES);
        bogus.putLong(10);
        bogus.putInt(Integer.MAX_VALUE);
        bogus.putInt(0);
        appendRawBytes(bogus.array());

        try (Log log = Log.open(dir, LogConfig.defaults())) {
            assertEquals(10, log.nextOffset());
        }
    }

    @Test
    void recoversAcrossSegmentRolls() throws IOException {
        LogConfig tiny = new LogConfig(64, 1024, FsyncPolicy.ALWAYS);
        try (Log log = Log.open(dir, tiny)) {
            for (int i = 0; i < 30; i++) {
                log.append(bytes("record-" + i));
            }
        }
        appendRawBytes(new byte[]{9, 9, 9}); // torn tail in the last of several segments

        try (Log log = Log.open(dir, tiny)) {
            assertEquals(30, log.nextOffset());
            List<LogRecord> all = log.readFrom(0, 100);
            assertEquals(30, all.size());
        }
    }

    @Test
    void recoversEmptyTailSegment() throws IOException {
        // simulates a crash immediately after a roll created the new file
        writeRecords(5);
        Files.createFile(dir.resolve(String.format("%020d.log", 5L)));

        try (Log log = Log.open(dir, LogConfig.defaults())) {
            assertEquals(5, log.nextOffset());
            assertEquals(5, log.append(bytes("next")));
        }
    }
}
