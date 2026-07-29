package com.minilog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * One append-only segment file. Record layout (see DESIGN.md):
 *
 *   offset   8 bytes
 *   length   4 bytes
 *   crc      4 bytes   CRC32 over offset + length + payload
 *   payload  n bytes
 */
final class LogSegment implements AutoCloseable {

    static final int HEADER_BYTES = 16;
    static final int INDEX_INTERVAL_BYTES = 4096;

    private final FileChannel channel;
    private final Path file;
    private final long baseOffset;
    private long endPosition;
    private long nextOffset;

    // sparse (offset, position) pairs roughly every INDEX_INTERVAL_BYTES,
    // built lazily on first read; purely a read accelerator (see DESIGN.md)
    private List<long[]> index;

    private LogSegment(FileChannel channel, Path file, long baseOffset, long endPosition) {
        this.channel = channel;
        this.file = file;
        this.baseOffset = baseOffset;
        this.endPosition = endPosition;
        this.nextOffset = baseOffset;
    }

    static LogSegment create(Path file, long baseOffset) throws IOException {
        FileChannel ch = FileChannel.open(file,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new LogSegment(ch, file, baseOffset, 0);
    }

    /**
     * Opens an existing segment without validating it. Sealed segments are
     * trusted (they were fsynced before the log rolled past them); the tail
     * segment must get a recover() call before use.
     */
    static LogSegment open(Path file, long baseOffset) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new LogSegment(ch, file, baseOffset, ch.size());
    }

    Path file() {
        return file;
    }

    long baseOffset() {
        return baseOffset;
    }

    long sizeBytes() {
        return endPosition;
    }

    long nextOffset() {
        return nextOffset;
    }

    void append(long offset, byte[] payload) throws IOException {
        long recordStart = endPosition;
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + payload.length);
        buf.putLong(offset);
        buf.putInt(payload.length);
        buf.putInt(crcOf(offset, payload));
        buf.put(payload);
        buf.flip();
        while (buf.hasRemaining()) {
            endPosition += channel.write(buf, endPosition);
        }
        nextOffset = offset + 1;
        if (index != null) {
            maybeIndex(offset, recordStart);
        }
    }

    /**
     * Scans from the start, validating every record, and truncates the file
     * at the first invalid one. Returns the offset after the last valid
     * record. See DESIGN.md "Recovery" for why each check exists.
     */
    long recover(int maxRecordBytes) throws IOException {
        long fileSize = channel.size();
        long pos = 0;
        long expected = baseOffset;

        while (pos + HEADER_BYTES <= fileSize) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
            readFully(header, pos);
            header.flip();
            long recOffset = header.getLong();
            int length = header.getInt();
            int storedCrc = header.getInt();

            if (recOffset != expected
                    || length < 0 || length > maxRecordBytes
                    || pos + HEADER_BYTES + length > fileSize) {
                break;
            }

            ByteBuffer payload = ByteBuffer.allocate(length);
            readFully(payload, pos + HEADER_BYTES);
            if (crcOf(recOffset, payload.array()) != storedCrc) {
                break;
            }

            pos += HEADER_BYTES + length;
            expected++;
        }

        if (pos < fileSize) {
            channel.truncate(pos);
        }
        endPosition = pos;
        nextOffset = expected;
        index = null; // truncation invalidates any indexed positions
        return expected;
    }

    /**
     * Appends records with offset >= fromOffset to out, at most max of them.
     * CRC is verified on every read so sealed-segment corruption fails loudly
     * instead of returning garbage.
     */
    int readInto(long fromOffset, int max, List<LogRecord> out) throws IOException {
        long pos = startPositionFor(fromOffset);
        int added = 0;

        while (pos + HEADER_BYTES <= endPosition && added < max) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
            readFully(header, pos);
            header.flip();
            long recOffset = header.getLong();
            int length = header.getInt();
            int storedCrc = header.getInt();

            if (length < 0 || pos + HEADER_BYTES + length > endPosition) {
                throw new IOException("corrupt record header at " + this + " position " + pos);
            }

            if (recOffset >= fromOffset) {
                ByteBuffer payload = ByteBuffer.allocate(length);
                readFully(payload, pos + HEADER_BYTES);
                if (crcOf(recOffset, payload.array()) != storedCrc) {
                    throw new IOException("CRC mismatch at offset " + recOffset);
                }
                out.add(new LogRecord(recOffset, payload.array()));
                added++;
            }
            pos += HEADER_BYTES + length;
        }
        return added;
    }

    /**
     * The best known file position at or before fromOffset's record.
     * Builds the index on first use with a headers-only scan.
     */
    private long startPositionFor(long fromOffset) throws IOException {
        if (index == null) {
            buildIndex();
        }
        long best = 0;
        for (long[] entry : index) { // sorted ascending; linear is fine at ~16 entries/64KB
            if (entry[0] > fromOffset) {
                break;
            }
            best = entry[1];
        }
        return best;
    }

    private void buildIndex() throws IOException {
        index = new ArrayList<>();
        long pos = 0;
        while (pos + HEADER_BYTES <= endPosition) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
            readFully(header, pos);
            header.flip();
            long recOffset = header.getLong();
            int length = header.getInt();
            if (length < 0 || pos + HEADER_BYTES + length > endPosition) {
                throw new IOException("corrupt record header at " + this + " position " + pos);
            }
            maybeIndex(recOffset, pos);
            pos += HEADER_BYTES + length;
        }
    }

    private void maybeIndex(long offset, long position) {
        if (index.isEmpty() || position - index.get(index.size() - 1)[1] >= INDEX_INTERVAL_BYTES) {
            index.add(new long[]{offset, position});
        }
    }

    void force() throws IOException {
        channel.force(false);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public String toString() {
        return "segment@" + baseOffset;
    }

    private void readFully(ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) {
                throw new IOException("unexpected end of segment at position " + pos);
            }
            pos += n;
        }
    }

    private static int crcOf(long offset, byte[] payload) {
        CRC32 crc = new CRC32();
        ByteBuffer prefix = ByteBuffer.allocate(12);
        prefix.putLong(offset);
        prefix.putInt(payload.length);
        prefix.flip();
        crc.update(prefix);
        crc.update(payload);
        return (int) crc.getValue();
    }
}
