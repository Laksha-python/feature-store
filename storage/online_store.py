import json
from pathlib import Path


def write_online_features(
    storage_dir,
    entities,
    user_features,
    product_features,
    net_revenue_features,
    reference_time
):

    online_dir = Path(storage_dir) / "online_store"
    online_dir.mkdir(parents=True, exist_ok=True)

    output_file = online_dir / "online_features.json"

    data = {}

    for e in entities:
        data[str(e)] = {}

        for fname, fmap in user_features.items():
            if e in fmap:
                data[str(e)][fname] = fmap[e]

        for fname, fmap in product_features.items():
            if e in fmap:
                data[str(e)][fname] = fmap[e]

        if e in net_revenue_features:
            data[str(e)]["net_revenue"] = net_revenue_features[e]

    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    print("Online store updated:", output_file)
