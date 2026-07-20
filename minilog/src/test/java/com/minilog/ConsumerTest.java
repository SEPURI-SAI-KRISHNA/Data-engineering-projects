package com.minilog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerTest {

    @TempDir
    Path dir;

    private Log log;
    private OffsetStore offsets;

    private void setup() throws IOException {
        log = Log.open(dir.resolve("log"), LogConfig.defaults());
        offsets = OffsetStore.open(dir.resolve("offsets"));
        for (int i = 0; i < 10; i++) {
            log.append(("record-" + i).getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void pollsFromTheStartByDefault() throws IOException {
        setup();
        Consumer consumer = Consumer.open(log, offsets, "g");

        List<LogRecord> batch = consumer.poll(3);
        assertEquals(3, batch.size());
        assertEquals(0, batch.get(0).offset());
        assertArrayEquals("record-0".getBytes(StandardCharsets.UTF_8), batch.get(0).payload());
    }

    @Test
    void pollAdvancesWithoutCommitting() throws IOException {
        setup();
        Consumer consumer = Consumer.open(log, offsets, "g");
        consumer.poll(4);
        consumer.poll(4);
        assertEquals(8, consumer.position());
        assertTrue(offsets.committed("g").isEmpty());
    }

    @Test
    void resumesFromCommittedOffset() throws IOException {
        setup();
        Consumer first = Consumer.open(log, offsets, "g");
        first.poll(4);
        first.commit();

        Consumer resumed = Consumer.open(log, offsets, "g");
        assertEquals(4, resumed.poll(1).get(0).offset());
    }

    @Test
    void uncommittedRecordsAreRedelivered() throws IOException {
        setup();
        Consumer first = Consumer.open(log, offsets, "g");
        first.poll(4);
        first.commit();
        first.poll(4); // consumed but never committed -- simulated crash here

        Consumer resumed = Consumer.open(log, offsets, "g");
        assertEquals(4, resumed.poll(1).get(0).offset(), "at-least-once: redelivery, not loss");
    }

    @Test
    void groupsConsumeIndependently() throws IOException {
        setup();
        Consumer a = Consumer.open(log, offsets, "group-a");
        Consumer b = Consumer.open(log, offsets, "group-b");
        a.poll(7);
        a.commit();

        assertEquals(0, b.poll(1).get(0).offset());
    }

    @Test
    void pollingPastTheEndReturnsEmpty() throws IOException {
        setup();
        Consumer consumer = Consumer.open(log, offsets, "g");
        consumer.poll(100);
        assertTrue(consumer.poll(100).isEmpty());

        log.append("late".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, consumer.poll(100).size(), "new records show up on the next poll");
    }
}
