from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

trips_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("service_id", StringType(), True),
    StructField("direction_id", IntegerType(), True),
    StructField("ingestion_date", DateType(), True)
])