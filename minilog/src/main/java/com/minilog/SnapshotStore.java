package com.minilog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Atomic state + offset snapshots for exactly-once processing.
 *
 * <p>Saves a consumer's processing state and its current log position together
 * in a single file write, using the tmp → fsync → rename → dir-fsync protocol
 * ({@link AtomicFile}). On restart, state and position are always in agreement
 * — a crash cannot produce a state that reflects records the position has not
 * yet acknowledged, or vice versa.
 *
 * <h2>File format</h2>
 * <pre>
 *   offset       8 bytes   log position where processing should resume
 *   stateLength  4 bytes   number of state bytes that follow
 *   crc          4 bytes   CRC32 over offset + stateLength + state
 *   state        n bytes   application state (opaque byte array)
 * </pre>
 *
 * <p>A missing or corrupt snapshot causes the consumer to restart with empty
 * state from offset 0 — wasteful but still exactly-once, because state and
 * position reset together.
 */
public final class SnapshotStore {

    private static final int HEADER_BYTES = 16; // offset(8) + stateLength(4) + crc(4)

    private final Path file;

    private SnapshotStore(Path file) {
        this.file = file;
    }

    /**
     * Opens a snapshot store backed by {@code file}.
     * Parent directories are created if they do not exist.
     */
    public static SnapshotStore open(Path file) throws IOException {
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        return new SnapshotStore(file);
    }

    /** A loaded snapshot: the persisted offset and the application state bytes. */
    public record Snapshot(long offset, byte[] state) {}

    /**
     * Loads the last saved snapshot, or empty if no valid snapshot exists
     * (missing file, truncated header, or CRC mismatch).
     */
    public Optional<Snapshot> load() throws IOException {
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long fileSize = ch.size();
            if (fileSize < HEADER_BYTES) {
                return Optional.empty();
            }

            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
            ch.read(header, 0);
            header.flip();

            long offset = header.getLong();
            int stateLength = header.getInt();
            int storedCrc = header.getInt();

            if (stateLength < 0 || fileSize < (long) HEADER_BYTES + stateLength) {
                return Optional.empty();
            }

            ByteBuffer stateBuf = ByteBuffer.allocate(stateLength);
            ch.read(stateBuf, HEADER_BYTES);
            stateBuf.flip();
            byte[] state = new byte[stateLength];
            stateBuf.get(state);

            if (computeCrc(offset, stateLength, state) != storedCrc) {
                return Optional.empty(); // corrupt snapshot — restart from scratch
            }

            return Optional.of(new Snapshot(offset, state));
        }
    }

    /**
     * Atomically persists {@code offset} and {@code state} together.
     * Returns only after the data is durable on disk.
     */
    public void save(long offset, byte[] state) throws IOException {
        int stateLength = state.length;
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + stateLength);
        buf.putLong(offset)
                .putInt(stateLength)
                .putInt(computeCrc(offset, stateLength, state))
                .put(state)
                .flip();
        AtomicFile.write(file, buf);
    }

    private static int computeCrc(long offset, int stateLength, byte[] state) {
        CRC32 crc = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(8 + 4 + state.length);
        buf.putLong(offset).putInt(stateLength).put(state).flip();
        crc.update(buf);
        return (int) crc.getValue();
    }
}
