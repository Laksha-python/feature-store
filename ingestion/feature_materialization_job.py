import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import csv
import logging
from datetime import datetime

from processing.feature_logic import (
    compute_user_features,
    compute_product_features,
    compute_net_revenue_features
)

from storage.offline_store import write_offline_feature
from storage.online_store import write_online_features
from storage.freshness_store import write_feature_freshness


BASE_DIR = PROJECT_ROOT
STORAGE_DIR = BASE_DIR / "storage"
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
    "net_revenue"
]

def load_events(csv_file):
    events = []

    with open(csv_file, "r", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                events.append({
                    "event_id": row["event_id"],
                    "event_type": row["event_type"],
                    "user_id": row["user_id"],
                    "product_id": row["product_id"],
                    "order_id": row["order_id"],
                    "price": float(row["price"]) if row["price"] else 0,
                    "event_time": row["event_time"]
                })
            except Exception as e:
                logging.warning(f"Skipping bad row: {e}")

    return events

def main():

    logging.info("Feature materialization job started")

    raw_dir = STORAGE_DIR / "raw_events"
    raw_files = sorted(raw_dir.glob("events_*.csv"))

    if not raw_files:
        logging.warning("No raw event files found")
        print("No raw event files found")
        return

    latest_file = raw_files[-1]

    logging.info(f"Reading raw events from {latest_file.name}")
    print(f"Reading raw events from {latest_file.name}")

    events = load_events(latest_file)

    if not events:
        logging.warning("No valid events found")
        print("No valid events found")
        return

    reference_time = datetime.now()
    feature_date = reference_time.strftime("%Y-%m-%d")

    user_features = compute_user_features(events, reference_time)
    product_features = compute_product_features(events, reference_time)
    net_revenue_features = compute_net_revenue_features(
        events,
        reference_time
    )
    for name, data in user_features.items():
        write_offline_feature(
            STORAGE_DIR,
            name,
            data,
            feature_date
        )

    for name, data in product_features.items():
        write_offline_feature(
            STORAGE_DIR,
            name,
            data,
            feature_date
        )

    write_offline_feature(
        STORAGE_DIR,
        "net_revenue",
        net_revenue_features,
        feature_date
    )

    all_entities = set()

    for d in user_features.values():
        all_entities |= set(d.keys())

    for d in product_features.values():
        all_entities |= set(d.keys())

    all_entities |= set(net_revenue_features.keys())

    write_online_features(
        STORAGE_DIR,
        all_entities,
        user_features,
        product_features,
        net_revenue_features,
        reference_time
    )

    write_feature_freshness(
        STORAGE_DIR,
        FEATURE_NAMES,
        reference_time
    )

    logging.info("Feature materialization completed successfully")
    print("✅ Feature materialization completed")


if __name__ == "__main__":
    main()
