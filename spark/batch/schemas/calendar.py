from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType

calendar_schema = StructType([
    StructField("service_id", StringType(), True),
    StructField("monday", BooleanType(), True),
    StructField("tuesday", BooleanType(), True),
    StructField("wednesday", BooleanType(), True),
    StructField("thursday", BooleanType(), True),
    StructField("friday", BooleanType(), True),
    StructField("saturday", BooleanType(), True),
    StructField("sunday", BooleanType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("ingestion_date", DateType(), True)
])