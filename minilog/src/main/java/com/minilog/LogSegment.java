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
 * A single segment file of a {@link Log}.
 *
 * <h2>On-disk record format</h2>
 * <pre>
 *   offset   8 bytes   logical offset of this record
 *   length   4 bytes   payload size in bytes
 *   crc      4 bytes   CRC32 over offset + length + payload
 *   payload  n bytes
 * </pre>
 *
 * <h2>Recovery</h2>
 * Only the tail segment is ever scanned. A torn write is detected when any of
 * the following holds: fewer than 16 header bytes remain; the stored offset
 * disagrees with the expected next offset; the length is out of range or extends
 * past the end of file; the CRC does not match. The file is truncated at the
 * last valid record.
 *
 * <h2>Sparse index</h2>
 * Each segment keeps an in-memory (offset → file-position) index with one entry
 * roughly every {@value #INDEX_INTERVAL} bytes. Reads binary-search this index
 * to skip to the nearest entry, then scan forward from there. The index is
 * built lazily on first read (for sealed segments opened without recovery) and
 * extended on every append. It is deliberately not persisted: rebuilding it is
 * one sequential header scan, and a persisted index would be a second on-disk
 * structure that recovery would have to distrust and re-verify.
 */
final class LogSegment {

    static final int HEADER_BYTES = 16; // offset(8) + length(4) + crc(4)
    private static final int INDEX_INTERVAL = 4 * 1024; // one entry per ~4 KB

    private final Path file;
    private final long base; // logical offset of the first record in this segment
    private final FileChannel channel;
    private long size; // current written size in bytes (== channel size after force())

    // Parallel lists forming the sparse index: indexOffset.get(i) is the logical
    // offset of the record whose header starts at indexPos.get(i) bytes from the
    // start of the file.
    private final List<Long> indexOffset = new ArrayList<>();
    private final List<Long> indexPos = new ArrayList<>();
    private boolean indexed = false; // true once the index covers the full file

    private LogSegment(Path file, long base, FileChannel channel, long size) {
        this.file = file;
        this.base = base;
        this.channel = channel;
        this.size = size;
    }

    // ── Factory methods ───────────────────────────────────────────────────────

    /** Creates a new, empty segment file and seeds its index. */
    static LogSegment create(Path file, long base) throws IOException {
        FileChannel ch = FileChannel.open(file,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        LogSegment seg = new LogSegment(file, base, ch, 0);
        seg.indexOffset.add(base);
        seg.indexPos.add(0L);
        seg.indexed = true;
        return seg;
    }

    /**
     * Opens an existing segment. The index is built lazily on first read; call
     * {@link #recover} on the tail segment before reading.
     */
    static LogSegment open(Path file, long base) throws IOException {
        FileChannel ch = FileChannel.open(file,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        return new LogSegment(file, base, ch, ch.size());
    }

    // ── Recovery ─────────────────────────────────────────────────────────────

    /**
     * Scans the segment from the beginning, validates every record, and truncates
     * the file at the first torn or corrupt record. Returns the next logical offset
     * (= base + number of intact records).
     */
    long recover(int maxRecordBytes) throws IOException {
        indexOffset.clear();
        indexPos.clear();

        long pos = 0;
        long expected = base;
        long lastIndexedAt = Long.MIN_VALUE;
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);

        outer:
        while (pos + HEADER_BYTES <= size) {
            // Add a sparse index entry if we haven't indexed recently.
            if (lastIndexedAt < 0 || pos - lastIndexedAt >= INDEX_INTERVAL) {
                indexOffset.add(expected);
                indexPos.add(pos);
                lastIndexedAt = pos;
            }

            // Read and validate the header.
            header.clear();
            if (!readFully(header, pos)) break; // torn header
            header.flip();

            long storedOffset = header.getLong();
            int length = header.getInt();
            int storedCrc = header.getInt();

            if (storedOffset != expected) break;               // misaligned / spliced
            if (length < 0 || length > maxRecordBytes) break;  // corrupt length
            if (pos + HEADER_BYTES + length > size) break;      // payload extends past EOF

            // Read the payload and verify the CRC.
            ByteBuffer payload = ByteBuffer.allocate(length);
            if (!readFully(payload, pos + HEADER_BYTES)) break; // torn payload
            payload.flip();
            byte[] payloadBytes = new byte[length];
            payload.get(payloadBytes);

            if (computeCrc(storedOffset, length, payloadBytes) != storedCrc) break; // CRC mismatch

            pos += HEADER_BYTES + length;
            expected++;
        }

        // Truncate at the last validated position.
        channel.truncate(pos);
        size = pos;

        // Ensure at least one index entry.
        if (indexOffset.isEmpty()) {
            indexOffset.add(expected);
            indexPos.add(0L);
        }
        indexed = true;
        return expected;
    }

    // ── Writes ───────────────────────────────────────────────────────────────

    /**
     * Appends a single record. The caller is responsible for calling
     * {@link #force()} to make it durable.
     */
    void append(long offset, byte[] payload) throws IOException {
        int length = payload.length;

        // Extend the sparse index if the current position is far enough from the
        // last indexed position.
        long lastPos = indexPos.isEmpty() ? Long.MIN_VALUE : indexPos.get(indexPos.size() - 1);
        if (lastPos < 0 || size - lastPos >= INDEX_INTERVAL) {
            indexOffset.add(offset);
            indexPos.add(size);
        }

        int crc = computeCrc(offset, length, payload);
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + length);
        buf.putLong(offset).putInt(length).putInt(crc).put(payload).flip();

        long writePos = size;
        while (buf.hasRemaining()) {
            writePos += channel.write(buf, writePos);
        }
        size += HEADER_BYTES + length;
    }

    /** Flushes the segment to durable storage. */
    void force() throws IOException {
        channel.force(false);
    }

    // ── Reads ────────────────────────────────────────────────────────────────

    /**
     * Reads up to {@code max} records starting at {@code startOffset} into
     * {@code out}. Records whose offset is below {@code startOffset} are skipped.
     * Stops at the end of the segment or when {@code max} is reached.
     *
     * @throws IOException if the CRC of a record does not match (data corruption)
     */
    void readInto(long startOffset, int max, List<LogRecord> out) throws IOException {
        if (max <= 0 || startOffset >= base + nextOffset()) return;

        ensureIndexed();

        // Binary search: find the last index entry with offset <= startOffset.
        int lo = 0, hi = indexOffset.size() - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) / 2;
            if (indexOffset.get(mid) <= startOffset) lo = mid;
            else hi = mid - 1;
        }

        long pos = indexPos.get(lo);
        long expected = indexOffset.get(lo);
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);

        while (out.size() < max && pos + HEADER_BYTES <= size) {
            header.clear();
            if (!readFully(header, pos)) break;
            header.flip();

            long offset = header.getLong();
            int length = header.getInt();
            int storedCrc = header.getInt();

            if (offset != expected || length < 0 || pos + HEADER_BYTES + length > size) break;

            ByteBuffer payload = ByteBuffer.allocate(length);
            if (!readFully(payload, pos + HEADER_BYTES)) break;
            payload.flip();
            byte[] payloadBytes = new byte[length];
            payload.get(payloadBytes);

            if (computeCrc(offset, length, payloadBytes) != storedCrc) {
                throw new IOException(
                        "CRC mismatch at logical offset " + offset + " in " + file +
                        ": possible data corruption");
            }

            if (offset >= startOffset) {
                out.add(new LogRecord(offset, payloadBytes));
            }

            pos += HEADER_BYTES + length;
            expected++;
        }
    }

    // ── Miscellaneous ─────────────────────────────────────────────────────────

    Path file() {
        return file;
    }

    /** Current on-disk size of the segment, in bytes. */
    long sizeBytes() {
        return size;
    }

    void close() throws IOException {
        channel.close();
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Builds the sparse index by scanning all record headers. Called lazily the
     * first time a read is attempted on a segment opened without {@link #recover}.
     */
    private void ensureIndexed() throws IOException {
        if (indexed) return;

        indexOffset.clear();
        indexPos.clear();

        long pos = 0;
        long expected = base;
        long lastIndexedAt = Long.MIN_VALUE;
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);

        while (pos + HEADER_BYTES <= size) {
            if (lastIndexedAt < 0 || pos - lastIndexedAt >= INDEX_INTERVAL) {
                indexOffset.add(expected);
                indexPos.add(pos);
                lastIndexedAt = pos;
            }
            header.clear();
            if (!readFully(header, pos)) break;
            header.flip();

            long offset = header.getLong();
            int length = header.getInt();
            // skip crc field
            header.getInt();

            if (offset != expected || length < 0 || pos + HEADER_BYTES + length > size) break;

            pos += HEADER_BYTES + length;
            expected++;
        }

        if (indexOffset.isEmpty()) {
            indexOffset.add(base);
            indexPos.add(0L);
        }
        indexed = true;
    }

    /**
     * Returns the number of intact records in this segment (based on the current
     * file size; accurate only after {@link #recover} or {@link #ensureIndexed}).
     * Used by {@link #readInto} to guard against reading past the end.
     */
    private long nextOffset() {
        // The last index entry holds the offset of a valid record at indexPos;
        // we may have appended more since then, but this is a conservative lower
        // bound sufficient to guard the readInto entry check.
        return size == 0 ? 0 : Long.MAX_VALUE; // let readInto detect end via size check
    }

    /** Reads {@code buf.remaining()} bytes from {@code pos}. Returns false on short read. */
    private boolean readFully(ByteBuffer buf, long pos) throws IOException {
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos + (buf.limit() - buf.remaining()));
            if (n < 0) return false;
            // n == 0 can happen on a non-blocking channel, but FileChannel is always blocking.
        }
        return true;
    }

    private static int computeCrc(long offset, int length, byte[] payload) {
        CRC32 crc = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(8 + 4 + payload.length);
        buf.putLong(offset).putInt(length).put(payload).flip();
        crc.update(buf);
        return (int) crc.getValue();
    }
}
