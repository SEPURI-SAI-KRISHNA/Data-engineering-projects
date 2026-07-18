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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String kafkaAddress = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics("transactions")
                .setGroupId("fraud-group")
                .setProperty("client.id", "fraud-consumer-" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        ObjectMapper mapper = new ObjectMapper();
        DataStream<Transaction> transactionStream = rawStream.map(json -> {
            try {
                return mapper.readValue(json, Transaction.class);
            } catch (Exception e) {
                return null; // drop malformed records
            }
        }).filter(t -> t != null);

        DataStream<String> alerts = transactionStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new CycleDetector());

        alerts.addSink(new AlertSink());

        env.execute("Fraud Ring Detector");
    }
}
