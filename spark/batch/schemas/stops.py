from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

stops_schema = StructType([
    StructField("stop_id", StringType(), True),
    StructField("stop_name", StringType(), True),
    StructField("stop_lat", DoubleType(), True),
    StructField("stop_lon", DoubleType(), True),
    StructField("ingestion_date", DateType(), True)
])