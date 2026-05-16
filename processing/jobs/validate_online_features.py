# -*- coding: utf-8 -*-
from pathlib import Path
import json
import csv
from datetime import datetime, timedelta
import sys


def load_freshness(storage_dir):
    freshness_file = Path(storage_dir) / "freshness" / "feature_freshness.csv"

    freshness = {}
    if not freshness_file.exists():
        print("Freshness file missing.")
        sys.exit(1)

    with open(freshness_file, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            freshness[row["feature_name"]] = row

    return freshness


def validate_online_features(storage_dir):
    online_dir = Path(storage_dir) / "online_store"
    freshness = load_freshness(storage_dir)

    issues = []

    if not online_dir.exists():
        print("Online store directory missing.")
        sys.exit(1)

    for entity_file in online_dir.glob("*.json"):
        with open(entity_file, "r", encoding="utf-8", errors="replace") as f:
            payload = json.load(f)

        for feature_name, value in payload.items():

            # Skip entity identifiers
            if feature_name in ["user_id", "entity_id"]:
                continue

            if feature_name not in freshness:
                issues.append(f"{feature_name}: missing freshness record")
                continue

            record = freshness[feature_name]
            last_updated = datetime.fromisoformat(record["last_updated_at"])
            expected = int(record["expected_frequency_hours"])

            age = datetime.now() - last_updated
            if age > timedelta(hours=expected):
                issues.append(f"{feature_name}: STALE")

    if issues:
        print("FEATURE VALIDATION FAILED")
        for issue in issues:
            print("-", issue)
        sys.exit(1)
    else:
        print("ALL FEATURES ARE FRESH AND VALID")
        sys.exit(0)


if __name__ == "__main__":
    validate_online_features("storage")