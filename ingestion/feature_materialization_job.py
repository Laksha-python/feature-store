import os
import logging
from datetime import datetime
from pathlib import Path

from processing.raw_event_reader import read_raw_events
from processing.feature_logic import (
    compute_event_count_last_7d,
    compute_purchase_count_last_30d,
    compute_avg_purchase_value_last_30d
)

from storage.offline_store import write_offline_feature
from storage.online_store import write_online_features
from storage.freshness_store import write_feature_freshness


BASE_DIR = Path(__file__).resolve().parents[1]
STORAGE_DIR = BASE_DIR / "storage"
LOG_DIR = BASE_DIR / "logs"

LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "feature_job.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


FEATURE_NAMES = [
    "user_event_count_last_7d",
    "user_purchase_count_last_30d",
    "user_avg_purchase_value_last_30d"
]


def main():
    logging.info("Feature materialization job started")

    raw_dir = STORAGE_DIR / "raw_events"
    raw_files = sorted(raw_dir.glob("events_*.csv"))

    if not raw_files:
        logging.warning("No raw event files found. Exiting.")
        return

    latest_file = raw_files[-1]
    logging.info(f"Reading raw events from {latest_file.name}")

    events = read_raw_events(latest_file)

    if not events:
        logging.warning("Raw event file contained no valid events. Exiting job.")
        return

    reference_time = datetime.now()
    feature_date = reference_time.strftime("%Y-%m-%d")

    event_counts_7d = compute_event_count_last_7d(events, reference_time)
    purchase_counts_30d = compute_purchase_count_last_30d(events, reference_time)
    avg_purchase_value_30d = compute_avg_purchase_value_last_30d(events, reference_time)

    write_offline_feature(
        STORAGE_DIR, "user_event_count_last_7d", event_counts_7d, feature_date
    )
    write_offline_feature(
        STORAGE_DIR, "user_purchase_count_last_30d", purchase_counts_30d, feature_date
    )
    write_offline_feature(
        STORAGE_DIR, "user_avg_purchase_value_last_30d", avg_purchase_value_30d, feature_date
    )

    users = set(
        event_counts_7d.keys()
        | purchase_counts_30d.keys()
        | avg_purchase_value_30d.keys()
    )

    write_online_features(
        STORAGE_DIR,
        users,
        event_counts_7d,
        purchase_counts_30d,
        avg_purchase_value_30d,
        reference_time
    )

    write_feature_freshness(STORAGE_DIR, FEATURE_NAMES, reference_time)

    logging.info("Feature materialization job completed successfully")


if __name__ == "__main__":
    main()
