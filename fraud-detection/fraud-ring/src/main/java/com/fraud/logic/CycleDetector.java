package com.fraud.logic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fraud.model.Transaction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Runs ring detection over each window and emits one JSON alert per window
 * that contains at least one ring, e.g.
 *
 *   {"windowStart":...,"windowEnd":...,"rings":[["A","B","C"]]}
 *
 * A single-parallelism window keeps the whole graph in one place, which is
 * acceptable at this scale. To scale out, key the stream by a coarse
 * partition (region, bank) and run detection per key.
 */
public class CycleDetector extends ProcessAllWindowFunction<Transaction, String, TimeWindow> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void process(Context context, Iterable<Transaction> elements, Collector<String> out) throws Exception {
        List<List<String>> rings = RingFinder.findRings(elements);
        if (rings.isEmpty()) {
            return;
        }

        ObjectNode alert = MAPPER.createObjectNode();
        alert.put("windowStart", context.window().getStart());
        alert.put("windowEnd", context.window().getEnd());
        alert.set("rings", MAPPER.valueToTree(rings));
        out.collect(MAPPER.writeValueAsString(alert));
    }
}
