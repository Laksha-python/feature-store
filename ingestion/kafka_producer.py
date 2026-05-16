import json
import time
import uuid
import random
import csv
from pathlib import Path
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

EVENT_TYPES = ["view", "purchase", "refund"]

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "storage" / "raw_events"
RAW_DIR.mkdir(parents=True, exist_ok=True)

def get_file():
    return RAW_DIR / f"events_{datetime.now().date()}.csv"

def write_event_to_csv(event):
    file_path = get_file()
    file_exists = file_path.exists()
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=event.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(event)

print("🚀 Kafka Producer started (Kafka + CSV)...")

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
    try:
        producer.send(TOPIC, value=event)
        producer.flush()
    except Exception as e:
        print("⚠️ Kafka error:", e)
    write_event_to_csv(event)
    print("📤 Event generated:", event["event_type"], event["user_id"], event["product_id"])

    time.sleep(1)