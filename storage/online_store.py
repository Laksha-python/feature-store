import json
from pathlib import Path
import os


def write_online_features(
    storage_dir,
    feature_name,
    feature_values,
    reference_time
):
    online_dir = Path(storage_dir) / "online_store"
    online_dir.mkdir(parents=True, exist_ok=True)

    tmp_file = online_dir / f".{feature_name}.tmp"
    final_file = online_dir / f"{feature_name}.json"

    payload = {
        "feature_name": feature_name,
        "updated_at": reference_time.isoformat(),
        "values": feature_values
    }

    with open(tmp_file, "w") as f:
        json.dump(payload, f, indent=2)

    os.replace(tmp_file, final_file)  

    print(f"Online store updated atomically: {final_file}")
