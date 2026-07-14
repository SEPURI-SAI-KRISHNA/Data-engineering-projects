package com.fraud.logic;

import com.fraud.model.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Finds payment rings (directed cycles) in a batch of transactions.
 *
 * Plain DFS with the current path kept on a stack. When we step onto a node
 * that is already on the path, the slice from its first occurrence to the top
 * of the stack is a ring. Every node is used as a DFS root, so rings that
 * share accounts are all found; duplicates are collapsed by rotating each
 * ring so its smallest account comes first.
 *
 * Window batches are small (a few seconds of traffic), so the lack of a
 * global visited set is fine. MAX_RINGS is a safety valve for degenerate
 * dense graphs.
 */
public final class RingFinder {

    private static final int MAX_RINGS = 100;

    private RingFinder() {
    }

    public static List<List<String>> findRings(Iterable<Transaction> transactions) {
        Map<String, Set<String>> graph = new HashMap<>();
        for (Transaction txn : transactions) {
            if (txn.senderAccount.equals(txn.receiverAccount)) {
                continue; // self-payments are not rings
            }
            graph.computeIfAbsent(txn.senderAccount, k -> new HashSet<>())
                 .add(txn.receiverAccount);
        }

        Set<List<String>> rings = new LinkedHashSet<>();
        for (String start : graph.keySet()) {
            if (rings.size() >= MAX_RINGS) {
                break;
            }
            dfs(start, graph, new ArrayList<>(), new HashMap<>(), rings);
        }
        return new ArrayList<>(rings);
    }

    private static void dfs(String node, Map<String, Set<String>> graph,
                            List<String> path, Map<String, Integer> onPath,
                            Set<List<String>> rings) {
        Integer firstSeen = onPath.get(node);
        if (firstSeen != null) {
            // Only the loop itself, not the tail we walked to reach it.
            rings.add(rotateToSmallest(path.subList(firstSeen, path.size())));
            return;
        }
        if (rings.size() >= MAX_RINGS) {
            return;
        }

        onPath.put(node, path.size());
        path.add(node);
        for (String next : graph.getOrDefault(node, Set.of())) {
            dfs(next, graph, path, onPath, rings);
        }
        path.remove(path.size() - 1);
        onPath.remove(node);
    }

    private static List<String> rotateToSmallest(List<String> ring) {
        int min = 0;
        for (int i = 1; i < ring.size(); i++) {
            if (ring.get(i).compareTo(ring.get(min)) < 0) {
                min = i;
            }
        }
        List<String> rotated = new ArrayList<>(ring.size());
        for (int i = 0; i < ring.size(); i++) {
            rotated.add(ring.get((min + i) % ring.size()));
        }
        return rotated;
    }
}
