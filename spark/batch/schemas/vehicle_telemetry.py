from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, LongType
)

vehicle_telemetry_schema = StructType([
    StructField("vehicle_id", StringType(), False),
    StructField("route_id", StringType(), False),
    StructField("timestamp", LongType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lon", DoubleType(), False),
    StructField("speed_kmh", IntegerType(), True),
    StructField("delay_seconds", IntegerType(), True),
    StructField("occupancy", IntegerType(), True),
])