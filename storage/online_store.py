import csv
import os


def write_online_features(
    storage_dir,
    users,
    event_counts_7d,
    purchase_counts_30d,
    avg_purchase_value_30d,
    computed_at
):
    base_path = os.path.join(storage_dir, "online_store")
    os.makedirs(base_path, exist_ok=True)

    file_path = os.path.join(base_path, "user_features.csv")

    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "user_id",
            "event_count_last_7d",
            "purchase_count_last_30d",
            "avg_purchase_value_last_30d",
            "computed_at"
        ])

        for user_id in sorted(users):
            avg_val = avg_purchase_value_30d.get(user_id)

            writer.writerow([
                user_id,
                event_counts_7d.get(user_id, 0),
                purchase_counts_30d.get(user_id, 0),
                round(avg_val, 2) if avg_val is not None else 0,
                computed_at.isoformat()
            ])
