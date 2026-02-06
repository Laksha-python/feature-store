import json
import csv
import os
from datetime import datetime
from kafka import KafkaConsumer


TOPIC = "user_events"
BOOTSTRAP_SERVERS = ["localhost:9092"]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "storage", "raw_events")

os.makedirs(RAW_DIR, exist_ok=True)


def normalize_event(event):
    try:
        return {
            "user_id": event["user_id"],
            "action_type": event["action_type"],
            "action_value": int(event["action_value"]),
            "event_timestamp": event["event_timestamp"]
        }
    except Exception:
        return None


def store_raw_events(events):
    if not events:
        return

    date_str = datetime.now().strftime("%Y-%m-%d")
    file_path = os.path.join(RAW_DIR, f"events_{date_str}.csv")
    file_exists = os.path.exists(file_path)

    with open(file_path, "a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "user_id",
                "action_type",
                "action_value",
                "event_timestamp"
            ]
        )

        if not file_exists:
            writer.writeheader()

        for event in events:
            writer.writerow(event)


def consume_and_store():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000
    )

    valid_events = []

    for msg in consumer:
        normalized = normalize_event(msg.value)
        if normalized:
            valid_events.append(normalized)

    store_raw_events(valid_events)
    consumer.close()

    print(f"Consumed & stored {len(valid_events)} events")


if __name__ == "__main__":
    consume_and_store()
