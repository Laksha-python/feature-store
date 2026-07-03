import json
from pathlib import Path
from datetime import datetime
import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="feature_store",
        user="user",
        password="password"
    )

def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_features (
            user_id TEXT,
            feature_name TEXT,
            value FLOAT,
            event_time TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_features (
            product_id TEXT,
            feature_name TEXT,
            value FLOAT,
            event_time TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ PostgreSQL tables ready")

def write_offline_feature_pg(
    feature_name,
    feature_values,
    entity_type,
    timestamp
):
    conn = get_connection()
    cur = conn.cursor()

    if entity_type == "user":
        for entity_id, value in feature_values.items():
            cur.execute(
                """
                INSERT INTO user_features (user_id, feature_name, value, event_time)
                VALUES (%s, %s, %s, %s)
                """,
                (entity_id, feature_name, float(value), timestamp)
            )

    elif entity_type == "product":
        for entity_id, value in feature_values.items():
            cur.execute(
                """
                INSERT INTO product_features (product_id, feature_name, value, event_time)
                VALUES (%s, %s, %s, %s)
                """,
                (entity_id, feature_name, float(value), timestamp)
            )

    conn.commit()
    cur.close()
    conn.close()

    print(f"📦 Written to Postgres → {feature_name}")


def write_offline_feature_json(
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

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"📁 JSON backup written: {output_file}")


def write_offline_feature(
    storage_dir,
    feature_name,
    feature_values,
    feature_date,
    entity_type="user"
):

    timestamp = datetime.now()
    write_offline_feature_pg(
        feature_name,
        feature_values,
        entity_type,
        timestamp
    )

    write_offline_feature_json(
        storage_dir,
        feature_name,
        feature_values,
        feature_date
    )