package com.minilog;

public record LogRecord(long offset, byte[] payload) {
}
