import json
import os
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
META_DIR = BASE_DIR / "metadata"
META_DIR.mkdir(exist_ok=True)

FEATURES_FILE = META_DIR / "features.json"
LINEAGE_FILE = META_DIR / "lineage.json"
SCHEMA_HISTORY_FILE = META_DIR / "schema_history.csv"


def _load_json(path):
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_feature_metadata():
    return _load_json(FEATURES_FILE)


def save_feature_metadata(metadata):
    _save_json(FEATURES_FILE, metadata)


def load_lineage():
    return _load_json(LINEAGE_FILE)


def save_lineage(lineage):
    _save_json(LINEAGE_FILE, lineage)


import csv

def record_schema(fields):
    if not SCHEMA_HISTORY_FILE.exists():
        with open(SCHEMA_HISTORY_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "fields"])

    last = None
    with open(SCHEMA_HISTORY_FILE, "r", newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if len(rows) > 1:
            last = rows[-1][1]

    encoded = ",".join(sorted(fields))
    if encoded != last:
        with open(SCHEMA_HISTORY_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([datetime.utcnow().isoformat(), encoded])


def read_schema_history():
    if not SCHEMA_HISTORY_FILE.exists():
        return []
    with open(SCHEMA_HISTORY_FILE, "r", newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        return list(reader)
