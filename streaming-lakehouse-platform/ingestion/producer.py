import time
import random

import msgspec
from confluent_kafka import Producer

EVENTS_PER_SEC = 100
SENSOR_COUNT = 10
TOPIC = 'telemetry_events'


class TelemetryEvent(msgspec.Struct):
    sensor_id: str
    temperature: float
    timestamp: int


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")


def generate_events():
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'telemetry-producer',
        'linger.ms': 5,
        'compression.type': 'lz4',
    })
    encoder = msgspec.json.Encoder()

    print(f"Producing to '{TOPIC}'. Ctrl+C to stop.")

    try:
        while True:
            event = TelemetryEvent(
                sensor_id=f"sensor_{random.randint(1, SENSOR_COUNT)}",
                temperature=round(random.uniform(20.0, 80.0), 2),
                timestamp=int(time.time() * 1000),
            )

            # keying by sensor_id keeps each sensor's events ordered
            # within a partition
            producer.produce(
                TOPIC,
                key=event.sensor_id.encode('utf-8'),
                value=encoder.encode(event),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(1 / EVENTS_PER_SEC)

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush()


if __name__ == '__main__':
    generate_events()
