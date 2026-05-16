import sys
from pathlib import Path
import csv
import logging
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))
from processing.feature_logic import (
    compute_user_features,
    compute_product_features,
    compute_net_revenue_features,
)

from storage.offline_store import write_offline_feature
from storage.online_store import write_online_snapshot
from storage.freshness_store import write_feature_freshness
from api.utils.redis_client import redis_set
BASE_DIR = PROJECT_ROOT
STORAGE_DIR = BASE_DIR / "storage"
RAW_DIR = STORAGE_DIR / "raw_events"
DLQ_DIR = STORAGE_DIR / "dlq"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / "feature_job.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
FEATURE_NAMES = [
    "rolling_7d_purchase_count",
    "rolling_30d_spend",
    "recency_days",
    "rolling_1h_sales",
    "rolling_24h_sales",
    "conversion_rate",
    "refund_rate",
    "net_revenue_30d",
    "error_rate_last_10min"
]

def load_events(csv_file):
    events = []
    with open(csv_file, "r", newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                events.append({
                    "event_id": row.get("event_id"),
                    "event_type": row.get("event_type"),
                    "user_id": row.get("user_id"),
                    "product_id": row.get("product_id"),
                    "order_id": row.get("order_id"),
                    "price": float(row["price"]) if row.get("price") else 0.0,
                    "event_time": row.get("event_time"),
                })
            except Exception as e:
                logging.warning(f"Skipping bad row: {e}")
    return events

def compute_error_rate():
    dlq_files = sorted(DLQ_DIR.glob("dlq_*.csv"))
    raw_files = sorted(RAW_DIR.glob("events_*.csv"))
    total_events = 0
    total_errors = 0

    for f in raw_files[-5:]:
        with open(f, "r", encoding="utf-8", errors="replace") as file:
            total_events += max(sum(1 for _ in file) - 1, 0)

    for f in dlq_files[-5:]:
        with open(f, "r", encoding="utf-8", errors="replace") as file:
            total_errors += max(sum(1 for _ in file) - 1, 0)

    total = total_events + total_errors
    return round((total_errors / total) if total > 0 else 0.0, 4)

def main():
    logging.info("Feature materialization job started")
    raw_files = sorted(RAW_DIR.glob("events_*.csv"))
    if not raw_files:
        print("No raw event files found")
        return

    latest_file = raw_files[-1]
    print(f"Reading raw events from {latest_file.name}")
    events = load_events(latest_file)
    if not events:
        print("No valid events found")
        return

    reference_time = datetime.now()
    feature_date = reference_time.strftime("%Y-%m-%d")
    user_features = compute_user_features(events, reference_time)
    product_features = compute_product_features(events, reference_time)
    net_revenue_features = compute_net_revenue_features(events, reference_time)
    error_rate = compute_error_rate()
    for name, data in user_features.items():
        write_offline_feature(STORAGE_DIR, name, data, feature_date)

    for name, data in product_features.items():
        write_offline_feature(STORAGE_DIR, name, data, feature_date)

    write_offline_feature(STORAGE_DIR, "net_revenue_30d", net_revenue_features, feature_date)
    write_offline_feature(STORAGE_DIR, "error_rate_last_10min", {"system": error_rate}, feature_date)

    snapshot = {
        "users": {},
        "products": {},
        "system": {}
    }
    for feature_name, feature_map in user_features.items():
        for user_id, value in feature_map.items():
            snapshot["users"].setdefault(user_id, {})
            snapshot["users"][user_id][feature_name] = value

    for feature_name, feature_map in product_features.items():
        for product_id, value in feature_map.items():
            snapshot["products"].setdefault(product_id, {})
            snapshot["products"][product_id][feature_name] = value

    for user_id, value in net_revenue_features.items():
        snapshot["users"].setdefault(user_id, {})
        snapshot["users"][user_id]["net_revenue_30d"] = value
    snapshot["system"]["error_rate_last_10min"] = error_rate

    write_online_snapshot(STORAGE_DIR, snapshot)

    print("⚡ Syncing features to Redis...")

    for user_id, features in snapshot["users"].items():
        redis_set(f"user:{user_id}", features, ttl=300)

    for product_id, features in snapshot["products"].items():
        redis_set(f"product:{product_id}", features, ttl=300)

    print("✅ Redis sync complete")

    write_feature_freshness(STORAGE_DIR, FEATURE_NAMES, reference_time)

    logging.info("Feature materialization completed successfully")
    print("✅ Feature materialization completed")


if __name__ == "__main__":
    main()