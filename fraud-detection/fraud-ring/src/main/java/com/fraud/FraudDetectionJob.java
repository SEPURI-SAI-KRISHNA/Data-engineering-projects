package com.fraud;

import com.fraud.logic.CycleDetector;
import com.fraud.model.Transaction;
import com.fraud.sink.AlertSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        // 1. Set up Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String kafkaAddress = "kafka:29092";

        // 2. Configure Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("transactions")
                .setGroupId("fraud-group")
                .setProperty("client.id", "fraud-consumer-" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3. Parse Strings to POJOs
        ObjectMapper mapper = new ObjectMapper();
        DataStream<Transaction> transactionStream = rawStream.map(json -> {
            try {
                return mapper.readValue(json, Transaction.class);
            } catch (Exception e) {
                return null; // Handle bad data gracefully
            }
        }).filter(t -> t != null);

        // 4. The Core Logic: Window -> Graph -> Alert
        DataStream<String> alerts = transactionStream
                // We assume global window for simplicity.
                // For high scale, keyBy(Transaction::getRegion)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new CycleDetector());

        // 5. Sink (Print for now, later Redis)
        //alerts.print();
        alerts.addSink(new AlertSink());

        env.execute("Fraud Ring Detector");
    }
}