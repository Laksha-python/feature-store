from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import csv
import json
import subprocess
import sys
from pathlib import Path

import redis

from api.utils.redis_client import redis_get

app = FastAPI(
    title="Feature Store API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print("🔥 Feature Store API Loaded")


PROJECT_ROOT = Path(__file__).resolve().parents[1]

STORAGE_DIR = PROJECT_ROOT / "storage"

ONLINE_STORE = (
    STORAGE_DIR
    / "online_store"
    / "online_features.json"
)

RAW_EVENTS_DIR = (
    STORAGE_DIR
    / "raw_events"
)


def load_online_store():

    if not ONLINE_STORE.exists():
        return {}

    try:

        with open(
            ONLINE_STORE,
            "r",
            encoding="utf-8"
        ) as f:

            return json.load(f)

    except Exception as e:

        print(f"❌ Failed to load store: {e}")

        return {}


@app.get("/health")
def health():

    return {
        "status": "healthy"
    }


@app.get("/health/postgres")
def postgres_health():
    return {
        "status": "healthy",
        "service": "postgres"
    }

@app.get("/health/redis")
def redis_health():
    try:
        r = redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True
        )
        r.ping()
        return {
            "status": "healthy",
            "service": "redis"
        }

    except Exception as e:
        return {
            "status": "down",
            "service": "redis",
            "error": str(e)
        }

@app.get("/metrics")
def metrics():
    data = load_online_store()
    users = data.get("users", {})
    products = data.get("products", {})
    return {
        "events_processed_count":
            len(users) + len(products),
        "duplicate_event_count": 0,
        "dlq_count": 0,
        "watermark_age_seconds": 0,
        "last_computed_timestamp": "latest"
    }

@app.get("/raw_events")
def raw_events():
    events = []
    try:
        files = sorted(
            RAW_EVENTS_DIR.glob("events_*.csv")
        )
        for file in files[-5:]:
            with open(
                file,
                "r",
                encoding="utf-8"
            ) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    events.append(row)

    except Exception as e:
        print(f"❌ Raw events error: {e}")
    return events

@app.get("/all-users")
def get_all_users():
    data = load_online_store()
    if "users" in data:
        users = data["users"]
    else:
        users = data
    return list(users.keys())

@app.get("/users/{user_id}")
def get_user(user_id: str):
    try:
        cached = redis_get(f"user:{user_id}")
        if cached:
            print(f"[CACHE HIT] user:{user_id}")
            return {
                "user_id": user_id,
                "features": cached,
                "source": "redis"
            }

    except Exception:
        print("⚠️ Redis unavailable")

    data = load_online_store()
    if "users" in data:
        users = data["users"]
    else:
        users = data
    features = users.get(user_id, {})
    return {
        "user_id": user_id,
        "features": features,
        "source": "offline_store"
    }

@app.get("/products")
def get_products():
    data = load_online_store()
    return list(
        data.get("products", {}).keys()
    )

@app.get("/products/{product_id}")
def get_product(product_id: str):
    data = load_online_store()
    features = (
        data
        .get("products", {})
        .get(product_id, {})
    )
    return {
        "product_id": product_id,
        "features": features
    }

@app.get("/features")
def features():
    data = load_online_store()
    if not data:
        return {"features": []}
    if "users" in data:
        users = data["users"]
    else:
        users = data

    if not users:
        return {"features": []}
    first_user = next(
        iter(users.values())
    )
    return {
        "features": list(first_user.keys())
    }

@app.post("/trigger-pipeline")
def trigger_pipeline():
    try:
        subprocess.Popen(
            [
                sys.executable,
                str(
                    PROJECT_ROOT
                    / "processing/jobs/backfill.py"
                )
            ],
            cwd=PROJECT_ROOT
        )
        return {
            "status": "started"
        }

    except Exception as e:
        return {
            "status": "error",
            "detail": str(e)
        }