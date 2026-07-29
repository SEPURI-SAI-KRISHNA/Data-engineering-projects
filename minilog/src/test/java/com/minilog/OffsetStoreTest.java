package com.minilog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetStoreTest {

    @TempDir
    Path dir;

    @Test
    void commitAndReadBack() throws IOException {
        OffsetStore store = OffsetStore.open(dir);
        store.commit("group-a", 42);
        assertEquals(OptionalLong.of(42), store.committed("group-a"));
    }

    @Test
    void survivesReopen() throws IOException {
        OffsetStore.open(dir).commit("group-a", 7);
        assertEquals(OptionalLong.of(7), OffsetStore.open(dir).committed("group-a"));
    }

    @Test
    void recommitReplacesOldOffset() throws IOException {
        OffsetStore store = OffsetStore.open(dir);
        store.commit("group-a", 5);
        store.commit("group-a", 10);
        assertEquals(OptionalLong.of(10), store.committed("group-a"));
    }

    @Test
    void groupsAreIndependent() throws IOException {
        OffsetStore store = OffsetStore.open(dir);
        store.commit("group-a", 1);
        store.commit("group-b", 2);
        assertEquals(OptionalLong.of(1), store.committed("group-a"));
        assertEquals(OptionalLong.of(2), store.committed("group-b"));
    }

    @Test
    void unknownGroupHasNoCommit() throws IOException {
        assertTrue(OffsetStore.open(dir).committed("nobody").isEmpty());
    }

    @Test
    void corruptCheckpointReadsAsNoCommit() throws IOException {
        OffsetStore store = OffsetStore.open(dir);
        store.commit("group-a", 42);
        Files.write(dir.resolve("group-a.ckpt"), new byte[]{1, 2, 3});

        // falling back to "never committed" means reprocessing, which
        // at-least-once allows; inventing an offset would skip records
        assertTrue(store.committed("group-a").isEmpty());
    }

    @Test
    void strayTmpFileFromACrashIsHarmless() throws IOException {
        OffsetStore store = OffsetStore.open(dir);
        store.commit("group-a", 42);
        Files.write(dir.resolve("group-a.tmp"), new byte[]{9, 9, 9});

        assertEquals(OptionalLong.of(42), store.committed("group-a"));
        store.commit("group-a", 43); // and the next commit overwrites it
        assertEquals(OptionalLong.of(43), store.committed("group-a"));
    }

    @Test
    void rejectsGroupNamesThatCouldEscapeTheDirectory() throws IOException {
        OffsetStore store = OffsetStore.open(dir);
        assertThrows(IllegalArgumentException.class, () -> store.commit("../evil", 1));
        assertThrows(IllegalArgumentException.class, () -> store.commit("a/b", 1));
        assertThrows(IllegalArgumentException.class, () -> store.commit("", 1));
    }
}
