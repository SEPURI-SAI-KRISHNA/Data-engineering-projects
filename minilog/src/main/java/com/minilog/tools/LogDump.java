package com.minilog.tools;

import com.minilog.Log;
import com.minilog.LogRecord;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Opens a log (running recovery in the process), checks every record's
 * payload against what ChaosWriter must have written, and prints one line
 * per record: "OK <offset>" or "BAD <offset>". The chaos harness parses
 * this output.
 */
public final class LogDump {

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        try (Log log = Log.open(dir, ChaosWriter.CONFIG)) {
            long offset = log.firstOffset();
            while (offset < log.nextOffset()) {
                List<LogRecord> batch = log.readFrom(offset, 1000);
                if (batch.isEmpty()) {
                    break;
                }
                for (LogRecord record : batch) {
                    boolean ok = Arrays.equals(record.payload(), ChaosWriter.payloadFor(record.offset()));
                    System.out.println((ok ? "OK " : "BAD ") + record.offset());
                }
                offset = batch.get(batch.size() - 1).offset() + 1;
            }
            System.out.println("NEXT " + log.nextOffset());
        }
    }
}
