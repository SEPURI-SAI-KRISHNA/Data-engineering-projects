package com.minilog;

import java.io.IOException;
import java.util.List;

/**
 * An at-least-once cursor over a log. poll() advances an in-memory position;
 * commit() makes it durable. Anything polled after the last commit is
 * re-delivered if the process crashes before committing — so commit after
 * processing, and make processing idempotent where possible. Committing
 * before processing flips the guarantee to at-most-once; that placement is
 * the caller's choice, not the library's.
 */
public final class Consumer {

    private final Log log;
    private final OffsetStore offsets;
    private final String group;
    private long position;

    private Consumer(Log log, OffsetStore offsets, String group, long position) {
        this.log = log;
        this.offsets = offsets;
        this.group = group;
        this.position = position;
    }

    /**
     * Resumes from the group's committed offset, or the start of the log.
     * If retention deleted records past the committed offset, the position
     * clamps forward to firstOffset() -- the records are gone, so jumping
     * ahead is the only honest option left.
     */
    public static Consumer open(Log log, OffsetStore offsets, String group) throws IOException {
        long start = Math.max(offsets.committed(group).orElse(0), log.firstOffset());
        return new Consumer(log, offsets, group, start);
    }

    public List<LogRecord> poll(int maxRecords) throws IOException {
        List<LogRecord> batch = log.readFrom(position, maxRecords);
        if (!batch.isEmpty()) {
            position = batch.get(batch.size() - 1).offset() + 1;
        }
        return batch;
    }

    /** Durably commits the current position. */
    public void commit() throws IOException {
        offsets.commit(group, position);
    }

    public long position() {
        return position;
    }
}
