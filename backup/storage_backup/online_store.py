import json
from pathlib import Path
def _init_store(data):
    if "users" not in data:
        data["users"] = {}
    if "products" not in data:
        data["products"] = {}
    return data

def write_online_snapshot(storage_dir, snapshot_data):
    online_dir = Path(storage_dir) / "online_store"
    online_dir.mkdir(parents=True, exist_ok=True)
    output_file = online_dir / "online_features.json"
    data = _init_store(snapshot_data)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print("✅ Online store snapshot updated:", output_file)

def update_online_store_batch(storage_dir, feature_name, feature_map):
    online_dir = Path(storage_dir) / "online_store"
    online_dir.mkdir(parents=True, exist_ok=True)
    output_file = online_dir / "online_features.json"
    if output_file.exists():
        try:
            with open(output_file, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
        except Exception:
            data = {}
    else:
        data = {}

    data = _init_store(data)
    user_features = {
        "rolling_7d_purchase_count",
        "rolling_30d_spend",
        "recency_days",
        "net_revenue_30d",
        "net_revenue_1h"
    }

    product_features = {
        "rolling_1h_sales",
        "rolling_24h_sales",
        "conversion_rate",
        "refund_rate"
    }

    if feature_name in user_features:
        entity_bucket = "users"
    elif feature_name in product_features:
        entity_bucket = "products"
    else:
        entity_bucket = "users"

    for entity_id, value in feature_map.items():
        entity_id = str(entity_id)
        if entity_id not in data[entity_bucket]:
            data[entity_bucket][entity_id] = {}
        data[entity_bucket][entity_id][feature_name] = value
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Online batch updated: {feature_name}")