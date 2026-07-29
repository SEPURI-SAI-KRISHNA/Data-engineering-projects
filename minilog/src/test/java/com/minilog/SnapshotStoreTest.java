package com.minilog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotStoreTest {

    @TempDir
    Path dir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void savesAndLoadsThePairTogether() throws IOException {
        SnapshotStore store = SnapshotStore.open(dir.resolve("sink.snap"));
        store.save(42, bytes("k0=7\nk1=3\n"));

        SnapshotStore.Snapshot snap = store.load().orElseThrow();
        assertEquals(42, snap.offset());
        assertArrayEquals(bytes("k0=7\nk1=3\n"), snap.state());
    }

    @Test
    void missingSnapshotLoadsAsEmpty() throws IOException {
        assertTrue(SnapshotStore.open(dir.resolve("nothing.snap")).load().isEmpty());
    }

    @Test
    void saveReplacesThePreviousSnapshot() throws IOException {
        SnapshotStore store = SnapshotStore.open(dir.resolve("sink.snap"));
        store.save(10, bytes("old"));
        store.save(20, bytes("new"));

        SnapshotStore.Snapshot snap = store.load().orElseThrow();
        assertEquals(20, snap.offset());
        assertArrayEquals(bytes("new"), snap.state());
    }

    @Test
    void corruptSnapshotLoadsAsEmpty() throws IOException {
        Path file = dir.resolve("sink.snap");
        SnapshotStore store = SnapshotStore.open(file);
        store.save(42, bytes("k0=7\n"));

        byte[] raw = Files.readAllBytes(file);
        raw[raw.length - 1] ^= 0x01;
        Files.write(file, raw);

        // empty means "restart from scratch": state and offset reset
        // together, so processing stays exact -- just slower
        assertTrue(store.load().isEmpty());
    }

    @Test
    void truncatedSnapshotLoadsAsEmpty() throws IOException {
        Path file = dir.resolve("sink.snap");
        Files.write(file, new byte[]{1, 2, 3});
        assertTrue(SnapshotStore.open(file).load().isEmpty());
    }

    @Test
    void strayTmpFileIsHarmless() throws IOException {
        Path file = dir.resolve("sink.snap");
        SnapshotStore store = SnapshotStore.open(file);
        store.save(42, bytes("good"));
        Files.write(dir.resolve("sink.snap.tmp"), bytes("half-written garbage"));

        assertEquals(42, store.load().orElseThrow().offset());
        store.save(43, bytes("still fine"));
        assertEquals(43, store.load().orElseThrow().offset());
    }

    @Test
    void emptyStateIsAValidSnapshot() throws IOException {
        SnapshotStore store = SnapshotStore.open(dir.resolve("sink.snap"));
        store.save(0, new byte[0]);

        SnapshotStore.Snapshot snap = store.load().orElseThrow();
        assertEquals(0, snap.offset());
        assertEquals(0, snap.state().length);
    }
}
