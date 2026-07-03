import json
from pathlib import Path
from datetime import datetime
import psycopg2


def get_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="feature_store",
        user="user",
        password="password"
    )


def write_offline_feature_pg(
    feature_name,
    feature_values,
    entity_type,
    timestamp
):
    conn=get_connection()
    cur=conn.cursor()
    feature_date=timestamp.date()
    if entity_type=="user":
        for entity_id,value in feature_values.items():
            cur.execute(
                """
                INSERT INTO user_features
                (
                    user_id,
                    feature_name,
                    feature_value,
                    feature_date
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, feature_name, feature_date)
                DO UPDATE SET
                    feature_value = EXCLUDED.feature_value,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    entity_id,
                    feature_name,
                    float(value),
                    feature_date
                )
            )

    elif entity_type=="product":
        for entity_id,value in feature_values.items():
            cur.execute(
                """
                INSERT INTO product_features
                (
                    product_id,
                    feature_name,
                    feature_value,
                    feature_date
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_id, feature_name, feature_date)
                DO UPDATE SET
                    feature_value = EXCLUDED.feature_value,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    entity_id,
                    feature_name,
                    float(value),
                    feature_date
                )
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
    feature_dir=(
        Path(storage_dir)
        /"offline_features"/feature_name
    )
    feature_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file=(
        feature_dir
        /f"{feature_date}.json"
    )
    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            feature_values,
            f,
            indent=2
        )

    print(f"💾 Written to JSON → {output_file.name}")

def write_offline_feature(
    storage_dir,
    feature_name,
    feature_values,
    feature_date,
    entity_type="user"
):
    timestamp=datetime.now()
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