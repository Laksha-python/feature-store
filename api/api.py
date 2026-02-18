import os
from fastapi import FastAPI, HTTPException
import json
from pathlib import Path
from datetime import datetime

app = FastAPI(title="Feature Store API")

BASE_DIR = Path(__file__).resolve().parent.parent
ONLINE_STORE = BASE_DIR / "storage" / "online_store"


@app.get("/features")
def list_features():
    features = []

    for file in ONLINE_STORE.glob("*.json"):
        with open(file, "r") as f:
            payload = json.load(f)

        features.append({
            "name": payload["feature_name"],
            "description": payload.get("description", "User-level feature"),
            "update_frequency": payload.get("update_frequency", "Daily"),
            "available_stores": ["online_store"]
        })

    return {"features": features}


@app.get("/features/{user_id}")
def get_user_features(user_id: str):
    result = {}

    for file in ONLINE_STORE.glob("*.json"):
        with open(file, "r") as f:
            payload = json.load(f)

        values = payload.get("values", {})
        if user_id in values:
            result[payload["feature_name"]] = values[user_id]

    if not result:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "user_id": user_id,
        "features": result,
        "computed_at": datetime.now().isoformat()
    }

@app.get("/health")
def health():
    return {"status":"ok"}

@app.post("/trigger-pipeline")
def trigger_pipeline():
    try:
        subprocess.run(
            ["docker", "exec", "airflow_orchestration-airflow-scheduler-1",
             "airflow", "dags", "trigger", "feature_platform_pipeline"],
            check=True,
        )
        return {"status": "triggered"}
    except Exception as e:
        return {"status": "failed", "error": str(e)}
@app.get("/all-users")

def get_all_users():
    path = "storage/online_store"

    features = {}
    for file in os.listdir(path):
        if file.endswith(".json"):
            with open(os.path.join(path, file)) as f:
                payload = json.load(f)

            feature_name = payload["feature_name"]
            for user, value in payload["values"].items():
                if user not in features:
                    features[user] = {"user_id": user}
                features[user][feature_name] = value

    return list(features.values())
