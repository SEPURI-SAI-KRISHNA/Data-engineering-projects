package com.minilog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * An atomic (offset, state) pair for exactly-once processing: the consumer
 * position and the state derived from everything before it are saved in one
 * atomically-renamed file, so they can never disagree after a crash.
 *
 * A missing or corrupt snapshot loads as empty, which restarts processing
 * from the beginning with empty state -- wasteful, but still exact, because
 * position and state reset together. See DESIGN.md "Exactly-once, honestly".
 */
public final class SnapshotStore {

    public record Snapshot(long offset, byte[] state) {
    }

    private static final int HEADER_BYTES = 16; // offset(8) + stateLength(4) + crc(4)

    private final Path file;

    private SnapshotStore(Path file) {
        this.file = file;
    }

    public static SnapshotStore open(Path file) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        return new SnapshotStore(file.toAbsolutePath());
    }

    /** Durably saves the pair. When this returns, both survive kill -9 -- or neither. */
    public synchronized void save(long offset, byte[] state) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + state.length);
        buf.putLong(offset);
        buf.putInt(state.length);
        buf.putInt(crcOf(offset, state));
        buf.put(state);
        buf.flip();
        AtomicFile.write(file, buf);
    }

    public synchronized Optional<Snapshot> load() throws IOException {
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        byte[] bytes = Files.readAllBytes(file);
        if (bytes.length < HEADER_BYTES) {
            return Optional.empty();
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        long offset = buf.getLong();
        int stateLength = buf.getInt();
        int storedCrc = buf.getInt();
        if (offset < 0 || stateLength != bytes.length - HEADER_BYTES) {
            return Optional.empty();
        }
        byte[] state = new byte[stateLength];
        buf.get(state);
        if (crcOf(offset, state) != storedCrc) {
            return Optional.empty();
        }
        return Optional.of(new Snapshot(offset, state));
    }

    private static int crcOf(long offset, byte[] state) {
        CRC32 crc = new CRC32();
        ByteBuffer prefix = ByteBuffer.allocate(12);
        prefix.putLong(offset);
        prefix.putInt(state.length);
        prefix.flip();
        crc.update(prefix);
        crc.update(state);
        return (int) crc.getValue();
    }
}
