import json
import time
import random
from kafka import KafkaProducer

# Config
TOPIC = "transactions"
PRODUCER = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def get_txn(src, dst, amt):
    return {
        "transactionId": str(random.randint(10000, 99999)),
        "senderAccount": src,
        "receiverAccount": dst,
        "amount": amt,
        "timestamp": int(time.time() * 1000)
    }


print("Starting Transaction Generator... (Press Ctrl+C to stop)")

try:
    while True:
        # 1. Generate normal "Noise" (Legitimate traffic)
        # Random people paying random people
        src = f"User{random.randint(1, 100)}"
        dst = f"User{random.randint(1, 100)}"
        if src != dst:
            PRODUCER.send(TOPIC, get_txn(src, dst, random.randint(10, 100)))
            print(f".", end="", flush=True)

        # 2. Occasionally inject a FRAUD RING (approx every 3 seconds)
        if random.random() < 0.1:
            print("\n[!] Injecting Fraud Ring: EvilA -> EvilB -> EvilC -> EvilA")

            # Send the ring sequence
            PRODUCER.send(TOPIC, get_txn("EvilA", "EvilB", 9000))
            PRODUCER.send(TOPIC, get_txn("EvilB", "EvilC", 9000))
            PRODUCER.send(TOPIC, get_txn("EvilC", "EvilA", 9000))

        time.sleep(0.3)  # Throttle speed

except KeyboardInterrupt:
    print("\nStopping.")