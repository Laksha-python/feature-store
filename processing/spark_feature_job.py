import sys
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from storage.offline_store import write_offline_feature


def main():
    spark = (
    SparkSession.builder
    .appName("FeatureComputation")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
    .getOrCreate()
)

    spark.sparkContext.setLogLevel("WARN")

    base_dir = Path(__file__).resolve().parents[1]
    storage_dir = base_dir / "storage"
    raw_dir = storage_dir / "raw_events"

    raw_files = sorted(raw_dir.glob("events_*.csv"))
    if not raw_files:
        print("No raw event files found")
        spark.stop()
        return

    latest_file = str(raw_files[-1])
    print(f"Reading raw events from {latest_file}")

    df = spark.read.csv(
        latest_file,
        header=True,
        inferSchema=True
    )

    reference_time = datetime.now()
    feature_date = reference_time.strftime("%Y-%m-%d")

    seven_days_ago = reference_time - timedelta(days=7)

    event_count_df = (
        df.filter(col("event_timestamp") >= seven_days_ago)
          .groupBy("user_id")
          .count()
          .withColumnRenamed("count", "event_count_last_7d")
    )

    event_counts_7d = {
        row["user_id"]: row["event_count_last_7d"]
        for row in event_count_df.collect()
    }

    write_offline_feature(
        str(storage_dir),
        "user_event_count_last_7d",
        event_counts_7d,
        feature_date
    )

    thirty_days_ago = reference_time - timedelta(days=30)

    purchase_count_df = (
        df.filter(
            (col("action_type") == "purchase") &
            (col("event_timestamp") >= thirty_days_ago)
        )
        .groupBy("user_id")
        .count()
        .withColumnRenamed("count", "purchase_count_last_30d")
    )

    purchase_counts_30d = {
        row["user_id"]: row["purchase_count_last_30d"]
        for row in purchase_count_df.collect()
    }

    write_offline_feature(
        str(storage_dir),
        "user_purchase_count_last_30d",
        purchase_counts_30d,
        feature_date
    )
    avg_purchase_df = (
        df.filter(
            (col("action_type") == "purchase") &
            (col("event_timestamp") >= thirty_days_ago)
        )
        .groupBy("user_id")
        .agg(avg("action_value").alias("avg_purchase_value_last_30d"))
    )

    avg_purchase_values_30d = {
        row["user_id"]: round(row["avg_purchase_value_last_30d"], 2)
        for row in avg_purchase_df.collect()
    }

    write_offline_feature(
        str(storage_dir),
        "user_avg_purchase_value_last_30d",
        avg_purchase_values_30d,
        feature_date
    )

    print("All Spark features computed and written successfully")
    spark.stop()


if __name__ == "__main__":
    main()
