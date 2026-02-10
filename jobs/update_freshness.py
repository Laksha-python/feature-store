from datetime import datetime
from storage.freshness_store import write_feature_freshness


def main():
    storage_dir = "storage"

    feature_names = [
        "user_event_count_last_7d",
        "user_purchase_count_last_30d",
        "user_avg_purchase_value_last_30d"
    ]

    write_feature_freshness(
        storage_dir=storage_dir,
        feature_names=feature_names,
        last_updated_at=datetime.now()
    )

    print("Feature freshness updated successfully")


if __name__ == "__main__":
    main()
