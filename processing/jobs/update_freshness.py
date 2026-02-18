from pathlib import Path
import csv
from datetime import datetime

STORAGE_DIR = "storage"
FRESHNESS_DIR = Path(STORAGE_DIR) / "freshness"
FRESHNESS_DIR.mkdir(parents=True, exist_ok=True)

FRESHNESS_FILE = FRESHNESS_DIR / "feature_freshness.csv"

FEATURES = [
    "user_event_count_last_7d",
    "user_purchase_count_last_30d",
    "user_avg_purchase_value_last_30d",
]

def update():
    now = datetime.now().isoformat()

    with open(FRESHNESS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "feature_name",
                "last_updated_at",
                "expected_frequency_hours"
            ]
        )

        writer.writeheader()

        for feature in FEATURES:
            writer.writerow({
                "feature_name": feature,
                "last_updated_at": now,
                "expected_frequency_hours": 24
            })

    print("Freshness metadata updated.")


if __name__ == "__main__":
    update()

