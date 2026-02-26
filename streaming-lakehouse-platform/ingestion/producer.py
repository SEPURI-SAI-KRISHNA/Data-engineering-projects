import time
import random
import msgspec
from confluent_kafka import Producer


# Define a strict, highly-optimized schema using msgspec
class TelemetryEvent(msgspec.Struct):
    sensor_id: str
    temperature: float
    timestamp: int


def delivery_report(err, msg):
    """Callback triggered by Kafka on successful or failed delivery."""
    if err is not None:
        print(f"Message delivery failed: {err}")


def generate_events():
    # Configure the Kafka Producer
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'telemetry-producer',
        'linger.ms': 5,  # Batching for higher throughput
        'compression.type': 'lz4'  # Fast compression algorithm
    }
    producer = Producer(conf)
    topic = 'telemetry_events'

    # Initialize the msgspec JSON encoder
    encoder = msgspec.json.Encoder()

    print(f"Starting ultra-low latency ingestion to topic '{topic}'...")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            # 1. Simulate a sensor event
            event = TelemetryEvent(
                sensor_id=f"sensor_{random.randint(1, 10)}",  # 10 distinct sensors
                temperature=round(random.uniform(20.0, 80.0), 2),
                timestamp=int(time.time() * 1000)
            )

            # 2. Ultra-fast serialization
            payload = encoder.encode(event)

            # 3. Produce to Kafka (Keying by sensor_id ensures ordered partitions)
            producer.produce(
                topic,
                key=event.sensor_id.encode('utf-8'),
                value=payload,
                callback=delivery_report
            )

            # Serve delivery callback queue
            producer.poll(0)

            # Control the velocity of the stream
            time.sleep(0.01)  # 100 events per second for testing

    except KeyboardInterrupt:
        print("\nStopping ingestion...")
    finally:
        # Wait for any outstanding messages to be delivered and delivery reports received
        print("Flushing final messages to Kafka...")
        producer.flush()


if __name__ == '__main__':
    generate_events()