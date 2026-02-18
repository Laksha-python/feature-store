import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    window,
    col,
    sum as spark_sum
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))


def main():

    spark = (
        SparkSession.builder
        .appName("Phase4_EventTimeStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    base_dir = Path(__file__).resolve().parents[1]
    raw_path = str(base_dir / "storage" / "raw_events")

    print("Reading streaming events from:", raw_path)

    schema = """
        event_id STRING,
        event_type STRING,
        user_id STRING,
        product_id STRING,
        order_id STRING,
        price INT,
        event_time TIMESTAMP,
        processing_time TIMESTAMP
    """

    df = (
        spark.readStream
        .schema(schema)
        .option("header", True)
        .csv(raw_path)
    )

    events = (
        df
        .filter(col("price").isNotNull())
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["order_id", "event_type"])
    )

    sales_1h = (
        events
        .filter(col("event_type") == "purchase")
        .groupBy(
            window(col("event_time"), "1 hour"),
            col("product_id")
        )
        .count()
    )
    purchase_count = (
        events
        .filter(col("event_type") == "purchase")
        .groupBy(
            window(col("event_time"), "30 minutes"),
            col("user_id")
        )
        .count()
    )

    spend_window = (
        events
        .filter(col("event_type") == "purchase")
        .groupBy(
            window(col("event_time"), "30 minutes"),
            col("user_id")
        )
        .agg(spark_sum("price").alias("rolling_spend"))
    )
    revenue_df = events.withColumn(
        "revenue_delta",
        when(col("event_type") == "purchase", col("price"))
        .when(col("event_type") == "refund", -col("price"))
        .when(col("event_type") == "cancel", -col("price"))
        .otherwise(0)
    )

    net_revenue_df = (
        revenue_df
        .groupBy(
            window(col("event_time"), "1 hour"),
            col("user_id")
        )
        .agg(
            spark_sum("revenue_delta").alias("net_revenue")
        )
    )
    def write_batch(batch_df, batch_id):
        rows = batch_df.collect()
        print(f"\n=== Batch {batch_id} ===")
        print("Rows:", len(rows))

        for r in rows[:5]:
            print(r)

    q1 = (
        sales_1h.writeStream
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/sales_1h")
        .foreachBatch(write_batch)
        .start()
    )

    q2 = (
        purchase_count.writeStream
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/purchase_count")
        .foreachBatch(write_batch)
        .start()
    )

    q3 = (
        spend_window.writeStream
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/spend_window")
        .foreachBatch(write_batch)
        .start()
    )

    q4 = (
        net_revenue_df.writeStream
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/net_revenue")
        .foreachBatch(write_batch)
        .start()
    )


    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
