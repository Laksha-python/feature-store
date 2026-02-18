import json
from pathlib import Path
from datetime import datetime


def write_offline_feature(
    storage_dir,
    feature_name,
    feature_values,
    feature_date
):
    base_dir = Path(storage_dir) / "offline_features" / feature_name
    base_dir.mkdir(parents=True, exist_ok=True)

    output_file = base_dir / f"{feature_date}.json"

    payload = {
        "feature_name": feature_name,
        "feature_date": feature_date,
        "computed_at": datetime.now().isoformat(),
        "values": feature_values
    }

    with open(output_file, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"Offline feature written: {output_file}")
