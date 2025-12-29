from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    count,
    when,
    date_format
)

spark = (
    SparkSession.builder
    .appName("BuildRoutePerformance")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

delay_facts_df = (
    spark.read
    .parquet("s3a://translink-d/curated/facts/delay_facts/")
)

# ------------------------------------------------------------------
# Add aggregation windows
# ------------------------------------------------------------------
with_time_df = (
    delay_facts_df
    .withColumn("service_date", date_format(col("event_time"), "yyyy-MM-dd"))
    .withColumn("service_hour", date_format(col("event_time"), "HH"))
)

# ------------------------------------------------------------------
# Route-level aggregation
# ------------------------------------------------------------------
route_perf_df = (
    with_time_df
    .groupBy("route_id", "service_date", "service_hour")
    .agg(
        count("*").alias("total_observations"),
        avg("delay_seconds").alias("avg_delay_seconds"),
        avg("speed_kmh").alias("avg_speed_kmh"),
        avg("occupancy").alias("avg_occupancy"),
        avg(
            when(col("delay_seconds") <= 60, 1).otherwise(0)
        ).alias("pct_on_time"),
        avg(
            when(col("delay_seconds") > 300, 1).otherwise(0)
        ).alias("pct_severe_delay"),
    )
)


(
    route_perf_df
    .write
    .mode("overwrite")
    .partitionBy("service_date")
    .parquet("s3a://translink-d/analytics/route_performance/")
)