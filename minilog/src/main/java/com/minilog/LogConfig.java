package com.minilog;

public record LogConfig(long segmentBytes, int maxRecordBytes, FsyncPolicy fsync) {

    public LogConfig {
        if (segmentBytes <= 0 || maxRecordBytes <= 0) {
            throw new IllegalArgumentException("sizes must be positive");
        }
    }

    public static LogConfig defaults() {
        return new LogConfig(64L * 1024 * 1024, 1024 * 1024, FsyncPolicy.ALWAYS);
    }
}
