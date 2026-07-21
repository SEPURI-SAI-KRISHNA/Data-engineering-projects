package com.minilog.tools;

import com.minilog.FsyncPolicy;
import com.minilog.Log;
import com.minilog.LogConfig;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Appends records forever and prints "ACK <offset>" after each one, until
 * the chaos harness SIGKILLs it. Payload content is a pure function of the
 * offset so the verifier (LogDump) can recompute what every record must
 * contain.
 *
 * Small segments on purpose: crashes should regularly land near segment
 * rolls, which is where the recovery invariant earns its keep.
 */
public final class ChaosWriter {

    static final LogConfig CONFIG = new LogConfig(256 * 1024, 1024 * 1024, FsyncPolicy.ALWAYS);

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        try (Log log = Log.open(dir, CONFIG)) {
            while (true) {
                long offset = log.append(payloadFor(log.nextOffset()));
                // stdout goes to a pipe; without the flush an ack could sit
                // in the buffer when we get killed. A lost ack line is safe
                // (we just don't verify that record), the reverse would not be.
                System.out.println("ACK " + offset);
                System.out.flush();
            }
        }
    }

    static byte[] payloadFor(long offset) {
        String body = "r" + offset + "-" + "x".repeat((int) (offset % 64));
        return body.getBytes(StandardCharsets.UTF_8);
    }
}
