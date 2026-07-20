package com.minilog;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A durable, offset-addressed record log backed by a directory of segment
 * files. Single writer. The ack contract and recovery rules are documented
 * in DESIGN.md; the short version: with FsyncPolicy.ALWAYS, once append()
 * returns, the record survives kill -9.
 */
public final class Log implements AutoCloseable {

    private final Path dir;
    private final LogConfig config;
    private final TreeMap<Long, LogSegment> segments = new TreeMap<>();
    private LogSegment active;
    private long nextOffset;
    private boolean failed;

    private Log(Path dir, LogConfig config) {
        this.dir = dir;
        this.config = config;
    }

    public static Log open(Path dir, LogConfig config) throws IOException {
        Files.createDirectories(dir);
        Log log = new Log(dir, config);

        List<Path> files;
        try (Stream<Path> s = Files.list(dir)) {
            files = s.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .sorted()
                    .collect(Collectors.toList());
        }

        if (files.isEmpty()) {
            log.active = LogSegment.create(dir.resolve(segmentFileName(0)), 0);
            log.fsyncDir();
            log.segments.put(0L, log.active);
            log.nextOffset = 0;
            return log;
        }

        for (Path file : files) {
            long base = baseOffsetOf(file);
            log.segments.put(base, LogSegment.open(file, base));
        }
        // sealed segments were fsynced before the log rolled past them, so
        // only the tail can be torn -- scan and repair just that one
        log.active = log.segments.lastEntry().getValue();
        log.nextOffset = log.active.recover(config.maxRecordBytes());
        return log;
    }

    /** Appends and returns the record's offset. This return is the ack. */
    public synchronized long append(byte[] payload) throws IOException {
        checkAppendable();
        checkSize(payload);
        try {
            long offset = appendOne(payload);
            if (config.fsync() == FsyncPolicy.ALWAYS) {
                active.force();
            }
            return offset;
        } catch (IOException e) {
            // after a failed write or fsync the page cache state is unknown;
            // retrying and acking would be lying (see DESIGN.md, fsyncgate)
            failed = true;
            throw e;
        }
    }

    /**
     * Appends every payload with a single fsync at the end (group commit).
     * Returns the offset of the first record; the rest follow contiguously.
     * The ack contract is unchanged: returning means the whole batch is
     * durable. All payloads are validated before any is written.
     */
    public synchronized long appendBatch(List<byte[]> payloads) throws IOException {
        checkAppendable();
        if (payloads.isEmpty()) {
            throw new IllegalArgumentException("empty batch");
        }
        for (byte[] payload : payloads) {
            checkSize(payload);
        }
        try {
            long first = nextOffset;
            for (byte[] payload : payloads) {
                appendOne(payload);
            }
            if (config.fsync() == FsyncPolicy.ALWAYS) {
                // a mid-batch roll already fsynced the sealed segment,
                // so forcing the active one covers everything
                active.force();
            }
            return first;
        } catch (IOException e) {
            failed = true;
            throw e;
        }
    }

    private long appendOne(byte[] payload) throws IOException {
        if (active.sizeBytes() >= config.segmentBytes()) {
            roll();
        }
        long offset = nextOffset;
        active.append(offset, payload);
        nextOffset = offset + 1;
        return offset;
    }

    private void checkAppendable() {
        if (failed) {
            throw new IllegalStateException("log is poisoned after a failed fsync; reopen to recover");
        }
    }

    private void checkSize(byte[] payload) {
        if (payload.length > config.maxRecordBytes()) {
            throw new IllegalArgumentException(
                    "record of " + payload.length + " bytes exceeds limit " + config.maxRecordBytes());
        }
    }

    /**
     * Reads up to maxRecords starting at offset. Empty list past the end;
     * offsets below firstOffset() (retention took them) are rejected loudly.
     */
    public synchronized List<LogRecord> readFrom(long offset, int maxRecords) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (offset < firstOffset()) {
            throw new IllegalArgumentException(
                    "offset " + offset + " was deleted by retention; log now starts at " + firstOffset());
        }
        List<LogRecord> out = new ArrayList<>();
        if (offset >= nextOffset) {
            return out;
        }
        long startSegment = segments.floorKey(offset);
        for (LogSegment segment : segments.tailMap(startSegment, true).values()) {
            segment.readInto(offset, maxRecords - out.size(), out);
            if (out.size() >= maxRecords) {
                break;
            }
        }
        return out;
    }

    public synchronized LogRecord read(long offset) throws IOException {
        List<LogRecord> records = readFrom(offset, 1);
        return records.isEmpty() ? null : records.get(0);
    }

    public synchronized long nextOffset() {
        return nextOffset;
    }

    /** The oldest offset still in the log (moves forward under retention). */
    public synchronized long firstOffset() {
        return segments.firstKey();
    }

    /**
     * Retention: deletes sealed segments whose entire range is below the
     * given offset. The active segment is never deleted. Deletes run
     * oldest-first with a directory fsync after each, so a crash can only
     * leave a contiguous suffix of segments. Returns segments deleted.
     */
    public synchronized int deleteUpTo(long offset) throws IOException {
        int deleted = 0;
        while (segments.size() > 1) {
            long firstBase = segments.firstKey();
            long nextBase = segments.higherKey(firstBase);
            if (nextBase > offset) {
                break; // this segment still holds offsets >= the cutoff
            }
            LogSegment victim = segments.get(firstBase);
            victim.close();
            Files.delete(victim.file());
            fsyncDir();
            segments.remove(firstBase);
            deleted++;
        }
        return deleted;
    }

    public synchronized void flush() throws IOException {
        active.force();
    }

    @Override
    public synchronized void close() throws IOException {
        for (LogSegment segment : segments.values()) {
            if (!failed) {
                segment.force();
            }
            segment.close();
        }
    }

    private void roll() throws IOException {
        active.force(); // seal the old tail so recovery never has to scan it
        LogSegment fresh = LogSegment.create(dir.resolve(segmentFileName(nextOffset)), nextOffset);
        fsyncDir(); // the new file's directory entry must be durable before we ack into it
        segments.put(nextOffset, fresh);
        active = fresh;
    }

    private void fsyncDir() throws IOException {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }

    private static String segmentFileName(long baseOffset) {
        return String.format("%020d.log", baseOffset);
    }

    private static long baseOffsetOf(Path file) {
        String name = file.getFileName().toString();
        return Long.parseLong(name.substring(0, name.length() - ".log".length()));
    }
}
