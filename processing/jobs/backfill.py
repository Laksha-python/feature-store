import sys
from pathlib import Path
from datetime import datetime
import json

PROJECT_ROOT=Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from processing.feature_logic import(
    compute_user_features,
    compute_product_features,
    compute_net_revenue_features
)

from storage.postgres_store import(
    write_user_features,
    write_product_features
)

STORAGE_DIR=PROJECT_ROOT/"storage"
RAW_EVENTS_FILE=STORAGE_DIR/"raw_events"/"events.json"
DLQ_FILE=STORAGE_DIR/"dlq"/"dlq.json"

def load_events():
    if not RAW_EVENTS_FILE.exists():
        print("No raw events found")
        return []

    with open(RAW_EVENTS_FILE,"r",encoding="utf-8") as f:
        return json.load(f)

def compute_metrics(events):
    seen_ids=set()
    duplicates=0
    valid_events=[]
    for event in events:
        eid=event.get("event_id")
        if eid in seen_ids:
            duplicates+=1
            continue

        seen_ids.add(eid)
        valid_events.append(event)
    total_events=len(events)
    if DLQ_FILE.exists():
        try:
            with open(DLQ_FILE,"r",encoding="utf-8") as f:
                dlq_events=json.load(f)
                dlq_count=len(dlq_events)
        except Exception:
            dlq_count=0
    else:
        dlq_count=0
    return {
        "total_events":total_events,
        "duplicates":duplicates,
        "dlq":dlq_count
    },valid_events

def main():
    print("🚀 Starting FULL BACKFILL")
    events=load_events()
    if not events:
        print("No raw events found")
        return

    metrics,clean_events=compute_metrics(events)
    print(f"Events: {metrics['total_events']}")
    print(f"Duplicates: {metrics['duplicates']}")
    print(f"DLQ: {metrics['dlq']}")

    reference_time=datetime.now()
    feature_date=reference_time.date()

    user_features=compute_user_features(
        clean_events,
        reference_time
    )

    product_features=compute_product_features(
        clean_events,
        reference_time
    )

    net_revenue_features=compute_net_revenue_features(
        clean_events,
        reference_time
    )

    print("📦 Writing user features to PostgreSQL...")
    for feature_name, feature_map in user_features.items():
        write_user_features(
            feature_name,
            feature_map,
            feature_date
        )

    print("📦 Writing product features to PostgreSQL...")
    for feature_name, feature_map in product_features.items():
        write_product_features(
            feature_name,
            feature_map,
            feature_date
        )

    write_user_features(
        "net_revenue_30d",
        net_revenue_features,
        feature_date
    )

    print("✅ PostgreSQL write complete")
    print("✅ BACKFILL COMPLETE")

if __name__=="__main__":
    main()