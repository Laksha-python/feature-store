from fastapi import FastAPI
from pathlib import Path
import json
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

app = FastAPI(
    title="Feature Store Observability API",
    version="1.0.0"
)

STORAGE_DIR = PROJECT_ROOT / "storage"
METRICS_FILE = (
    STORAGE_DIR
    / "metrics"
    / "metrics_store.json"
)

@app.get("/health")
def health():
    return {
        "status": "healthy"
    }

@app.get("/health/metrics")
def metrics_health():
    return {
        "metrics_file_exists":
            METRICS_FILE.exists(),

        "metrics_file":
            str(METRICS_FILE)
    }

@app.get("/metrics")
def get_metrics():
    if not METRICS_FILE.exists():
        return {
            "error": "metrics file not found"
        }
    try:
        with open(
            METRICS_FILE,
            "r",
            encoding="utf-8",
            errors="replace"
        ) as f:
            data = json.load(f)
        return data

    except Exception as e:
        return {
            "error": str(e)
        }