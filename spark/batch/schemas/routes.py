from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

routes_schema = StructType([
    StructField("route_id", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("route_short_name", StringType(), True),
    StructField("route_long_name", StringType(), True),
    StructField("route_type", IntegerType(), True),
    StructField("ingestion_date", DateType(), True)
])