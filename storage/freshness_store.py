import csv
import os
from datetime import datetime, timedelta


def write_feature_freshness(storage_dir, feature_names, last_updated_at):
    base_path = os.path.join(storage_dir, "freshness")
    os.makedirs(base_path, exist_ok=True)

    file_path = os.path.join(base_path, "feature_freshness.csv")

    expected_frequency_hours = 24
    now = datetime.now()

    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "feature_name",
            "last_updated_at",
            "expected_frequency_hours",
            "status"
        ])

        for feature in feature_names:
            age = now - last_updated_at
            status = (
                "fresh"
                if age <= timedelta(hours=expected_frequency_hours)
                else "stale"
            )

            writer.writerow([
                feature,
                last_updated_at.isoformat(),
                expected_frequency_hours,
                status
            ])
