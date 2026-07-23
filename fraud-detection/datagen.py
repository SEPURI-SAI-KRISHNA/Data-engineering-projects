import json
import time
import random
from kafka import KafkaProducer

TOPIC = "transactions"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def txn(src, dst, amt):
    return {
        "transactionId": str(random.randint(10000, 99999)),
        "senderAccount": src,
        "receiverAccount": dst,
        "amount": amt,
        "timestamp": int(time.time() * 1000),
    }


def inject_ring():
    """Pick 3-5 regular accounts and cycle money through them."""
    size = random.randint(3, 5)
    members = random.sample([f"User{i}" for i in range(1, 101)], size)
    amount = random.randint(5000, 15000)
    print(f"\n[!] Injecting ring: {' -> '.join(members)} -> {members[0]}")
    for i, src in enumerate(members):
        dst = members[(i + 1) % size]
        producer.send(TOPIC, txn(src, dst, amount))


print("Starting transaction generator... (Ctrl+C to stop)")

try:
    while True:
        # background noise: random users paying random users
        src = f"User{random.randint(1, 100)}"
        dst = f"User{random.randint(1, 100)}"
        if src != dst:
            producer.send(TOPIC, txn(src, dst, random.randint(10, 100)))
            print(".", end="", flush=True)

        # roughly one ring every few seconds
        if random.random() < 0.1:
            inject_ring()

        time.sleep(0.3)

except KeyboardInterrupt:
    print("\nStopping.")
    producer.flush()
