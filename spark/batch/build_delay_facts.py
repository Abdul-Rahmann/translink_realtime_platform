from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_date,
    hour,
    when,
    current_timestamp
)

from schemas.vehicle_telemetry import vehicle_telemetry_schema

from dotenv import load_dotenv 

load_dotenv()


RAW_TELEMETRY_PATH = "s3a://translink-d/raw/vehicle_telemetry/"
OUTPUT_PATH = "s3a://translink-d/curated/facts/delay_facts/"


# ------------------------------------------------------------------
# Spark Session
# ------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("build_delay_facts")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------------
# Read raw telemetry
# ------------------------------------------------------------------
telemetry_df = (
    spark.read
    .schema(vehicle_telemetry_schema)
    .parquet(RAW_TELEMETRY_PATH)
)


# ------------------------------------------------------------------
# Timestamp normalization
# ------------------------------------------------------------------
telemetry_df = (
    telemetry_df
    .withColumn(
        "event_time",
        from_unixtime(col("timestamp")).cast("timestamp")
    )
    .withColumn("service_date", to_date(col("event_time")))
    .withColumn("service_hour", hour(col("event_time")))
)


# ------------------------------------------------------------------
# Delay classification logic
# ------------------------------------------------------------------
delay_facts_df = (
    telemetry_df
    .withColumn(
        "delay_status",
        when(col("delay_seconds") >= 300, "SEVERE")
        .when(col("delay_seconds") >= 60, "MINOR")
        .when(col("delay_seconds") >= 0, "ON_TIME")
        .otherwise("EARLY")
    )
    .withColumn(
        "is_delayed",
        when(col("delay_seconds") >= 60, 1).otherwise(0)
    )
    .withColumn("ingestion_ts", current_timestamp())
)


# ------------------------------------------------------------------
# Final fact table projection
# ------------------------------------------------------------------
final_df = delay_facts_df.select(
    col("vehicle_id"),
    col("route_id"),
    col("event_time"),
    col("service_date"),
    col("service_hour"),
    col("delay_seconds"),
    col("delay_status"),
    col("is_delayed"),
    col("speed_kmh"),
    col("occupancy"),
    col("ingestion_ts")
)


# ------------------------------------------------------------------
# Write curated output
# ------------------------------------------------------------------
(
    final_df
    .write
    .mode("append")
    .partitionBy("service_date")
    .parquet(OUTPUT_PATH)
)


spark.stop()