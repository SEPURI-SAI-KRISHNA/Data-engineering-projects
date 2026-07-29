package com.minilog;

/**
 * Controls when segment data is flushed to durable storage.
 *
 * <p>The difference between the two modes is the benchmark:
 * ALWAYS costs ~2.5 ms per flush and is the ack contract;
 * OS hands flushing to the kernel and is ~1600× faster but survives only
 * a process kill, not a power failure. See BENCHMARKS.md for measured numbers.
 */
public enum FsyncPolicy {
    /** FileChannel.force(true) after every append (or every batch). Durable before returning. */
    ALWAYS,
    /** Rely on the OS page-cache flush. Fast; not power-loss safe. For benchmarking only. */
    OS
}
