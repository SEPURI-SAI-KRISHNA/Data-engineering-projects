package com.minilog;

public enum FsyncPolicy {
    /** fsync before every ack. append() returning means the record is on disk. */
    ALWAYS,
    /** leave flushing to the OS. Fast, but a crash can lose acked records. */
    OS
}
