from pathlib import Path
from datetime import datetime, UTC

from storage.offline_store import load_offline_feature
from storage.online_store import write_online_features


def main():
    base_dir = Path(__file__).resolve().parents[1]
    storage_dir = base_dir / "storage"
    offline_root = storage_dir / "offline_features"

    reference_time = datetime.now(UTC)

    for feature_dir in offline_root.iterdir():
        if not feature_dir.is_dir():
            continue

        feature_name = feature_dir.name
        print(f"Loading offline feature: {feature_name}")

        feature_values, source_file = load_offline_feature(feature_dir)

        write_online_features(
            storage_dir=storage_dir,
            feature_name=feature_name,
            feature_values=feature_values,
            reference_time=reference_time
        )


if __name__ == "__main__":
    main()
