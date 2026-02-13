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

    with open(freshness_file, "r") as f:
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

    for feature_file in online_dir.glob("*.json"):
        with open(feature_file, "r") as f:
            payload = json.load(f)

        name = payload["feature_name"]

        if name not in freshness:
            issues.append(f"{name}: missing freshness record")
            continue

        record = freshness[name]
        last_updated = datetime.fromisoformat(record["last_updated_at"])
        expected = int(record["expected_frequency_hours"])

        age = datetime.now() - last_updated
        if age > timedelta(hours=expected):
            issues.append(f"{name}: STALE")

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

