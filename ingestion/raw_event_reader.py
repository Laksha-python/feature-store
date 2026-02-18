import json
import csv
import os
import time
from datetime import datetime
from kafka import KafkaConsumer

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(PROJECT_ROOT, "storage", "raw_events")

os.makedirs(RAW_DIR, exist_ok=True)

consumer = KafkaConsumer(
    "events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

BUFFER = []
BATCH_SIZE = 50       
FLUSH_SECONDS = 10     

last_flush = time.time()

print("🚀 Consumer started (micro-batch mode)")

def flush_buffer():
    global BUFFER, last_flush

    if not BUFFER:
        return

    filename = datetime.now().strftime("events_%Y%m%d_%H%M%S.csv")
    filepath = os.path.join(RAW_DIR, filename)

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=BUFFER[0].keys())
        writer.writeheader()
        writer.writerows(BUFFER)

    print(f"✅ Wrote {len(BUFFER)} events → {filename}")

    BUFFER = []
    last_flush = time.time()


for msg in consumer:
    BUFFER.append(msg.value)

    now = time.time()

    if len(BUFFER) >= BATCH_SIZE or (now - last_flush) > FLUSH_SECONDS:
        flush_buffer()
