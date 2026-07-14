package com.fraud.logic;

import com.fraud.model.Transaction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingFinderTest {

    private static Transaction txn(String from, String to) {
        return new Transaction("t", from, to, 100.0, 0L);
    }

    @Test
    void findsASimpleRing() {
        List<List<String>> rings = RingFinder.findRings(List.of(
                txn("A", "B"), txn("B", "C"), txn("C", "A")));

        assertEquals(List.of(List.of("A", "B", "C")), rings);
    }

    @Test
    void findsTwoDisjointRings() {
        List<List<String>> rings = RingFinder.findRings(List.of(
                txn("A", "B"), txn("B", "A"),
                txn("X", "Y"), txn("Y", "Z"), txn("Z", "X")));

        assertEquals(2, rings.size());
        assertTrue(rings.contains(List.of("A", "B")));
        assertTrue(rings.contains(List.of("X", "Y", "Z")));
    }

    @Test
    void findsRingsThatShareAnAccount() {
        // Two rings both passing through B. The old detector's global
        // visited set made it miss the second one.
        List<List<String>> rings = RingFinder.findRings(List.of(
                txn("A", "B"), txn("B", "A"),
                txn("B", "C"), txn("C", "B")));

        assertEquals(2, rings.size());
        assertTrue(rings.contains(List.of("A", "B")));
        assertTrue(rings.contains(List.of("B", "C")));
    }

    @Test
    void reportsOnlyTheLoopNotTheTailLeadingToIt() {
        // A pays into the ring but is not part of it.
        List<List<String>> rings = RingFinder.findRings(List.of(
                txn("A", "B"), txn("B", "C"), txn("C", "B")));

        assertEquals(List.of(List.of("B", "C")), rings);
    }

    @Test
    void ignoresSelfPaymentsAndAcyclicTraffic() {
        List<List<String>> rings = RingFinder.findRings(List.of(
                txn("A", "A"), txn("A", "B"), txn("B", "C")));

        assertTrue(rings.isEmpty());
    }

    @Test
    void deduplicatesTheSameRingSeenFromDifferentStarts() {
        // Duplicate edges should not create duplicate ring reports.
        List<Transaction> txns = new ArrayList<>(List.of(
                txn("A", "B"), txn("B", "C"), txn("C", "A"),
                txn("B", "C"), txn("C", "A")));

        assertEquals(1, RingFinder.findRings(txns).size());
    }
}
