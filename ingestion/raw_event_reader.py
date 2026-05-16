import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import json
import csv
import os
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer

from storage.metrics_store import update_metrics

from processing.feature_logic import (
    compute_user_features,
    compute_product_features
)
from api.utils.redis_client import redis_set

RAW_DIR = os.path.join(PROJECT_ROOT, "storage", "raw_events")
DLQ_DIR = os.path.join(PROJECT_ROOT, "storage", "dlq")
STATE_DIR = os.path.join(PROJECT_ROOT, "storage", "state")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(DLQ_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

VALID_EVENT_TYPES = {"view", "purchase", "refund", "cancel"}

BUFFER = []
DLQ_BUFFER = []

BATCH_SIZE = 50
FLUSH_SECONDS = 10

TOTAL_EVENTS = 0
ERROR_EVENTS = 0

last_flush = time.time()

PROCESSED_IDS_FILE = os.path.join(STATE_DIR, "processed_ids.json")

if os.path.exists(PROCESSED_IDS_FILE):
    with open(PROCESSED_IDS_FILE, "r", encoding="utf-8", errors="replace") as f:
        PROCESSED_IDS = set(json.load(f))
else:
    PROCESSED_IDS = set()

print("🚀 Consumer started (Realtime Feature + Redis Enabled)")

consumer = KafkaConsumer(
    "events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

def validate_event(event):

    if not event.get("user_id"):
        return False, "missing_user_id"

    if event.get("event_type") not in VALID_EVENT_TYPES:
        return False, "invalid_event_type"

    try:
        float(event.get("price", 0))
    except Exception:
        return False, "price_not_numeric"

    return True, None


def is_late_event(event):
    try:
        event_time = datetime.fromisoformat(event["event_time"])
        if event_time < datetime.utcnow() - timedelta(minutes=10):
            return True
    except Exception:
        return False
    return False


def flush_dlq():
    global DLQ_BUFFER

    if not DLQ_BUFFER:
        return

    filename = datetime.now().strftime("dlq_%Y%m%d_%H%M%S.csv")
    filepath = os.path.join(DLQ_DIR, filename)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DLQ_BUFFER[0].keys())
        writer.writeheader()
        writer.writerows(DLQ_BUFFER)

    print(f"❌ DLQ wrote {len(DLQ_BUFFER)} bad events")

    update_metrics(os.path.join(PROJECT_ROOT, "storage"), "dlq_count", len(DLQ_BUFFER))

    DLQ_BUFFER = []


def flush_buffer():
    global BUFFER, last_flush

    if not BUFFER:
        return

    filename = datetime.now().strftime("events_%Y%m%d_%H%M%S.csv")
    filepath = os.path.join(RAW_DIR, filename)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=BUFFER[0].keys())
        writer.writeheader()
        writer.writerows(BUFFER)

    print(f"📁 Wrote {len(BUFFER)} valid events → {filename}")

    update_metrics(os.path.join(PROJECT_ROOT, "storage"), "events_processed_count", len(BUFFER))

    try:
        times = [datetime.fromisoformat(e["event_time"]) for e in BUFFER]
        max_time = max(times)
        age = (datetime.utcnow() - max_time).total_seconds()
        update_metrics(os.path.join(PROJECT_ROOT, "storage"), "watermark_age_seconds", age)
    except Exception:
        pass

    BUFFER = []
    last_flush = time.time()

    with open(PROCESSED_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(PROCESSED_IDS), f)


def print_error_rate():
    if TOTAL_EVENTS == 0:
        return
    rate = ERROR_EVENTS / TOTAL_EVENTS
    print(f"⚠️ error_rate_last_10min = {rate:.2%}")


for msg in consumer:

    event = msg.value
    TOTAL_EVENTS += 1

    event_id = event.get("event_id")

    if event_id in PROCESSED_IDS:
        update_metrics(os.path.join(PROJECT_ROOT, "storage"), "duplicate_event_count", 1)
        continue

    PROCESSED_IDS.add(event_id)

    if is_late_event(event):
        update_metrics(os.path.join(PROJECT_ROOT, "storage"), "late_event_count", 1)

    valid, reason = validate_event(event)

    if valid:
        BUFFER.append(event)
        try:
            reference_time = datetime.utcnow()
            recent_events = BUFFER[-100:]
            user_features = compute_user_features(recent_events, reference_time)
            user_id = event["user_id"]

            user_feat = {
                "rolling_7d_purchase_count": user_features["rolling_7d_purchase_count"].get(user_id, 0),
                "rolling_30d_spend": user_features["rolling_30d_spend"].get(user_id, 0),
                "recency_days": user_features["recency_days"].get(user_id, 0),
            }

            redis_set(f"user:{user_id}", user_feat, ttl=300)

            product_features = compute_product_features(recent_events, reference_time)
            product_id = event["product_id"]

            prod_feat = {
                "rolling_1h_sales": product_features["rolling_1h_sales"].get(product_id, 0),
                "rolling_24h_sales": product_features["rolling_24h_sales"].get(product_id, 0),
                "conversion_rate": product_features["conversion_rate"].get(product_id, 0),
                "refund_rate": product_features["refund_rate"].get(product_id, 0),
            }

            redis_set(f"product:{product_id}", prod_feat, ttl=300)

            print(f"⚡ Redis updated → user:{user_id}, product:{product_id}")

        except Exception as e:
            print("❌ Feature update failed:", e)

    else:
        ERROR_EVENTS += 1
        event["error_reason"] = reason
        DLQ_BUFFER.append(event)

    now = time.time()

    if len(BUFFER) >= BATCH_SIZE or (now - last_flush) > FLUSH_SECONDS:
        flush_buffer()
        flush_dlq()
        print_error_rate()