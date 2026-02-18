import json
import time
import uuid
import random
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

EVENT_TYPES = ["view", "purchase", "refund"]

print("🚀 Producer started...")

while True:

    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": f"user_{random.randint(1,10)}",
        "product_id": f"product_{random.randint(1,5)}",
        "order_id": str(uuid.uuid4()),
        "price": random.randint(100, 1000),
        "event_time": datetime.now().isoformat(),
        "processing_time": datetime.now().isoformat(),
    }

    producer.send(TOPIC, value=event)
    producer.flush()

    print("📤 Sent:", event)

    time.sleep(1)
