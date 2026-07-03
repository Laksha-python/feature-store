import json
from pathlib import Path


def _metrics_file(storage_dir):
    metrics_dir = Path(storage_dir) / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    return metrics_dir / "metrics_store.json"


def update_metrics(storage_dir, metric_name, increment_value=1):
    file_path = _metrics_file(storage_dir)
    if file_path.exists():
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
        except Exception:
            data = {}
    else:
        data = {}

    data[metric_name] = data.get(metric_name, 0) + increment_value
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def read_metrics(storage_dir):
    file_path = _metrics_file(storage_dir)
    if not file_path.exists():
        return {}
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    except Exception:
        return {}