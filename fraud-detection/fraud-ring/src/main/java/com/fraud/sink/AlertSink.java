package com.fraud.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class AlertSink extends RichSinkFunction<String> {

    private transient JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) {
        // Connect to the Redis container defined in docker-compose (hostname: 'redis')
        // If running locally in IDE, use "localhost". If inside Docker, use "redis".
        // We use a simple logic to detect where we are running.
        String redisHost = System.getenv("FLINK_PROPERTIES") != null ? "redis" : "localhost";
        jedisPool = new JedisPool(redisHost, 6379);
    }

    @Override
    public void invoke(String value, Context context) {
        try (Jedis jedis = jedisPool.getResource()) {
            // Store in a Redis List named "fraud_alerts"
            jedis.lpush("fraud_alerts", value);

            // Also publish to a channel for real-time dashboards to subscribe to
            jedis.publish("fraud_notifications", value);
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}