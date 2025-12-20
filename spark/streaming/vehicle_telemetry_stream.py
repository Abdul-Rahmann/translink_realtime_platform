from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, when, from_unixtime,
    current_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)

spark = (
    SparkSession.builder
    .appName('VehicleTelemetryStreaming')
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')

schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("route_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("speed_kmh", IntegerType()),
    StructField("delay_seconds", IntegerType()),
    StructField("occupancy", IntegerType())
])

raw_df = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'localhost:9092')
    .option('subscribe', 'vehicle_telemetry')
    .option('startingOffsets', 'latest')
    .load()
)

parsed_df = (
    raw_df
    .selectExpr('CAST(value AS STRING) AS json_str')
    .select(from_json(col('json_str'), schema).alias('data'))
    .select('data.*')
)

enriched_df = (
    parsed_df
    .withColumn(
    "event_time",
    from_unixtime(col("timestamp")).cast("timestamp"))
    .withColumn(
        'delay_status',
        when(col('delay_seconds') > 300, 'SEVERE')
        .when(col('delay_seconds') > 60, 'MINOR')
        .otherwise('ON_TIME')       
    )
)

final_df = (
    enriched_df
    .withColumn("ingest_time", current_timestamp())
    .withColumn("ingest_date", date_format("ingest_time", "yyyy-MM-dd"))
    .withColumn("ingest_hour", date_format("ingest_time", "HH"))
)

query = (
    final_df
    .writeStream
    .format("parquet")
    # .format("console")
    .outputMode("append")
    .partitionBy("ingest_date", "ingest_hour")
    .option(
        "path",
        "s3a://translink-d/raw/vehicle_telemetry/"
    )
    .option(
        "checkpointLocation",
        "s3a://translink-d/checkpoints/vehicle_telemetry_v5/"
    )
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()