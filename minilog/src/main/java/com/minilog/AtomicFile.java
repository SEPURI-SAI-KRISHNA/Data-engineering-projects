package com.minilog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * The tmp -> fsync -> rename -> dir-fsync protocol shared by offset
 * checkpoints and sink snapshots. A crash leaves the old file or the new
 * one, never a torn mix.
 */
final class AtomicFile {

    private AtomicFile() {
    }

    static void write(Path file, ByteBuffer content) throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            while (content.hasRemaining()) {
                ch.write(content);
            }
            ch.force(true);
        }
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE);
        try (FileChannel dir = FileChannel.open(file.getParent(), StandardOpenOption.READ)) {
            dir.force(true);
        }
    }
}
