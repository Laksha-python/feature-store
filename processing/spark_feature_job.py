import sys
from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, udf, sum as spark_sum
from pyspark.sql.types import DoubleType

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from processing.feature_logic import revenue_delta
from storage.offline_store import write_offline_feature
from storage.online_store import update_online_store_batch


def main():

    spark = (
        SparkSession.builder
        .appName("FeatureStore_Streaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    base_dir = PROJECT_ROOT
    raw_path = str(base_dir / "storage" / "raw_events")
    storage_dir = str(base_dir / "storage")

    print("Reading streaming events from:", raw_path)

    schema = """
        event_id STRING,
        event_type STRING,
        user_id STRING,
        product_id STRING,
        order_id STRING,
        price DOUBLE,
        event_time TIMESTAMP
    """

    df = (
        spark.readStream
        .schema(schema)
        .option("header", True)
        .csv(raw_path)
    )

    events = df.withWatermark("event_time", "10 minutes")
    revenue_udf = udf(revenue_delta, DoubleType())

    revenue_df = events.withColumn(
        "revenue_delta",
        revenue_udf(col("event_type"), col("price"))
    )
    net_revenue_df = (
        revenue_df
        .groupBy(
            window(col("event_time"), "1 hour"),
            col("user_id")
        )
        .agg(spark_sum("revenue_delta").alias("net_revenue_1h"))
    )

    sales_1h_df = (
        events
        .filter(col("event_type") == "purchase")
        .groupBy(
            window(col("event_time"), "1 hour"),
            col("product_id")
        )
        .count()
        .withColumnRenamed("count", "rolling_1h_sales")
    )


    def write_batch(feature_name, key_col):

        def _writer(batch_df, batch_id):

            rows = batch_df.collect()

            print(f"\n=== Batch {batch_id} :: {feature_name} ===")
            print("Rows:", len(rows))

            if not rows:
                return

            data = {}

            for r in rows:
                key = r[key_col]

                for col_name, value in r.asDict().items():
                    if col_name not in ["window", key_col]:
                        if value is not None:
                            data[str(key)] = float(value)

            if not data:
                return

            write_offline_feature(
                storage_dir,
                feature_name,
                data,
                "streaming"
            )

            update_online_store_batch(
                storage_dir,
                feature_name,
                data
            )

            online_file = Path(storage_dir) / "online_store" / "online_features.json"

            try:
                if online_file.exists():
                    with open(online_file, "r") as f:
                        store = json.load(f)
                else:
                    store = {}

                store.setdefault("system", {})
                store["system"]["events_processed_count"] = (
                    store["system"].get("events_processed_count", 0)
                    + len(rows)
                )

                store["system"]["last_computed"] = str(batch_id)

                with open(online_file, "w") as f:
                    json.dump(store, f, indent=2)

            except Exception as e:
                print("⚠️ Failed updating system metrics:", e)

        return _writer

    q1 = (
        net_revenue_df.writeStream
        .outputMode("update")
        .foreachBatch(write_batch("net_revenue_1h", "user_id"))
        .start()
    )

    q2 = (
        sales_1h_df.writeStream
        .outputMode("update")
        .foreachBatch(write_batch("rolling_1h_sales", "product_id"))
        .start()
    )

    print("\n🚀 STREAMING RUNNING")
    print("Press CTRL+C to stop")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()