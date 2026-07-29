package com.minilog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * Durable committed offsets, one checkpoint file per consumer group.
 *
 * Each commit writes a tmp file, fsyncs it, atomically renames it over the
 * checkpoint, and fsyncs the directory — a crash leaves the old offset or
 * the new one, never a torn file. A checkpoint that is missing or fails its
 * CRC reads as "no commit", which restarts the group from 0: with
 * at-least-once delivery, falling backwards is the safe direction.
 *
 * See DESIGN.md for why this isn't stored in the log itself.
 */
public final class OffsetStore {

    private static final Pattern GROUP_NAME = Pattern.compile("[A-Za-z0-9._-]{1,100}");
    private static final int CHECKPOINT_BYTES = 12; // offset(8) + crc(4)

    private final Path dir;

    private OffsetStore(Path dir) {
        this.dir = dir;
    }

    public static OffsetStore open(Path dir) throws IOException {
        Files.createDirectories(dir);
        return new OffsetStore(dir);
    }

    /** Durably records the offset. When this returns, the commit survives kill -9. */
    public synchronized void commit(String group, long offset) throws IOException {
        checkGroupName(group);
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }

        ByteBuffer buf = ByteBuffer.allocate(CHECKPOINT_BYTES);
        buf.putLong(offset);
        buf.putInt(crcOf(offset));
        buf.flip();
        AtomicFile.write(dir.resolve(group + ".ckpt"), buf);
    }

    /** The last committed offset, or empty if the group never committed. */
    public synchronized OptionalLong committed(String group) throws IOException {
        checkGroupName(group);
        Path ckpt = dir.resolve(group + ".ckpt");
        if (!Files.exists(ckpt)) {
            return OptionalLong.empty();
        }

        byte[] bytes = Files.readAllBytes(ckpt);
        if (bytes.length != CHECKPOINT_BYTES) {
            return OptionalLong.empty();
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        long offset = buf.getLong();
        int storedCrc = buf.getInt();
        if (offset < 0 || crcOf(offset) != storedCrc) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(offset);
    }

    private static void checkGroupName(String group) {
        if (!GROUP_NAME.matcher(group).matches()) {
            throw new IllegalArgumentException("invalid group name: " + group);
        }
    }

    private static int crcOf(long offset) {
        CRC32 crc = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(offset);
        buf.flip();
        crc.update(buf);
        return (int) crc.getValue();
    }
}
