package com.fraud.logic;

import com.fraud.model.Transaction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

// We use ProcessAllWindow to see the GLOBAL view of the graph for a short window.
// In hyper-scale, you would partition this by Region/Country.
public class CycleDetector extends ProcessAllWindowFunction<Transaction, String, TimeWindow> {

    @Override
    public void process(Context context, Iterable<Transaction> elements, Collector<String> out) {
        // 1. Build the Adjacency List for this window
        Map<String, List<String>> graph = new HashMap<>();
        List<Transaction> txns = new ArrayList<>();

        for (Transaction txn : elements) {
            graph.computeIfAbsent(txn.senderAccount, k -> new ArrayList<>()).add(txn.receiverAccount);
            txns.add(txn);
        }

        // 2. Detect Cycles (DFS)
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();
        List<String> detectedCycles = new ArrayList<>();

        for (String node : graph.keySet()) {
            if (hasCycle(node, graph, visited, recursionStack, new ArrayList<>(), detectedCycles)) {
                // Cycle found starting from this node
            }
        }

        // 3. Output Alerts
        if (!detectedCycles.isEmpty()) {
            String alert = String.format("CRITICAL: Fraud Ring Detected in Window %s: %s",
                context.window(), String.join(" | ", detectedCycles));
            out.collect(alert);
            System.out.println(alert); // Print to console for now
        }
    }

    // Standard DFS Cycle Detection
    private boolean hasCycle(String node, Map<String, List<String>> graph,
                             Set<String> visited, Set<String> recursionStack,
                             List<String> path, List<String> results) {

        if (recursionStack.contains(node)) {
            // Cycle detected! Add the node to close the loop
            path.add(node);
            results.add(path.toString());
            path.remove(path.size() - 1); // backtrack
            return true;
        }
        if (visited.contains(node)) {
            return false;
        }

        visited.add(node);
        recursionStack.add(node);
        path.add(node);

        List<String> neighbors = graph.get(node);
        if (neighbors != null) {
            for (String neighbor : neighbors) {
                if (hasCycle(neighbor, graph, visited, recursionStack, path, results)) {
                    // Continue finding other cycles
                }
            }
        }

        recursionStack.remove(node);
        path.remove(path.size() - 1);
        return false;
    }
}