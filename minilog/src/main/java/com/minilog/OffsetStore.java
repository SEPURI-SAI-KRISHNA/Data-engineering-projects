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
 * Durable committed-offset checkpoints for consumer groups.
 *
 * <p>Each group gets its own file: {@code <dir>/<group>.ckpt}. The file holds
 * exactly 12 bytes: the committed offset (8 bytes, big-endian) followed by its
 * CRC32 (4 bytes). Writes use the tmp → fsync → rename → dir-fsync protocol
 * ({@link AtomicFile}) so a crash always leaves either the old checkpoint or
 * the new one, never a torn file.
 *
 * <p>A missing or corrupt checkpoint causes the consumer to restart from offset
 * 0 — the at-least-once safe direction. Falling backwards (re-delivery) is
 * always safe; falling forwards (skipping records) is not.
 */
public final class OffsetStore {

    private static final int CHECKPOINT_BYTES = 12; // offset(8) + crc(4)

    private final Path dir;

    private OffsetStore(Path dir) {
        this.dir = dir;
    }

    /**
     * Opens (or creates) an offset store rooted at {@code dir}.
     * The directory is created if it does not already exist.
     */
    public static OffsetStore open(Path dir) throws IOException {
        Files.createDirectories(dir);
        return new OffsetStore(dir);
    }

    /**
     * Returns the last committed offset for {@code group}, or empty if no
     * durable checkpoint exists (missing file or corrupt CRC).
     */
    public Optional<Long> committed(String group) throws IOException {
        Path file = dir.resolve(group + ".ckpt");
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            if (ch.size() < CHECKPOINT_BYTES) {
                return Optional.empty();
            }
            ByteBuffer buf = ByteBuffer.allocate(CHECKPOINT_BYTES);
            ch.read(buf, 0);
            buf.flip();

            long offset = buf.getLong();
            int storedCrc = buf.getInt();

            if (crc(offset) != storedCrc) {
                return Optional.empty(); // corrupt checkpoint — restart from 0
            }
            return Optional.of(offset);
        }
    }

    /**
     * Durably commits {@code offset} for {@code group}. Returns only after the
     * data is safe on disk (via {@link AtomicFile}).
     */
    public void commit(String group, long offset) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(CHECKPOINT_BYTES);
        buf.putLong(offset).putInt(crc(offset)).flip();
        AtomicFile.write(dir.resolve(group + ".ckpt"), buf);
    }

    private static int crc(long offset) {
        CRC32 c = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(offset).flip();
        c.update(buf);
        return (int) c.getValue();
    }
}
