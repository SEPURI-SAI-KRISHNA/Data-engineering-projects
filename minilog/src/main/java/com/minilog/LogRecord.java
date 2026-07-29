package com.minilog;

/**
 * An immutable, offset-addressed record returned by {@link Log#readFrom}.
 *
 * <p>{@code offset} is the logical position of the record in the log (guaranteed
 * globally unique and monotonically increasing). {@code payload} is the raw bytes
 * that were passed to {@link Log#append} or {@link Log#appendBatch}.
 */
public record LogRecord(long offset, byte[] payload) {}
