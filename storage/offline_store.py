import csv
import os


def write_offline_feature(storage_dir, feature_name, feature_values, feature_date):
    base_path = os.path.join(
        storage_dir,
        "offline_features",
        feature_name,
        f"date={feature_date}"
    )
    os.makedirs(base_path, exist_ok=True)

    file_path = os.path.join(base_path, "data.csv")

    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["user_id", "feature_value", "feature_date"])

        for user_id, value in feature_values.items():
            writer.writerow([user_id, value, feature_date])
