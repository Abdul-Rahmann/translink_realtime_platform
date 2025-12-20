from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date, col
from datetime import datetime

from schemas.calendar import calendar_schema
from schemas.routes import routes_schema
from schemas.stops import stops_schema
from schemas.trips import trips_schema  

S3_RAW = "s3a://translink-d/raw/gtfs"
S3_CURATED = "s3a://translink-d/curated/gtfs"
INGESTION_DATE = datetime.today().strftime("%Y-%m-%d")

spark = (
    SparkSession.builder.appName("GTFSBatchTransformation")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')

def read_gtfs(file_name):
    return (
        spark.read.option('header', 'true')
        .csv(f"{S3_RAW}/ingestion_date={INGESTION_DATE}/{file_name}").
        withColumn('ingestion_date', to_date(lit(INGESTION_DATE)))
    )

routes_df = read_gtfs("routes.txt")
routes_df.select(
    routes_schema.fieldNames()) \
    .write.mode('overwrite') \
    .parquet(f"{S3_CURATED}/routes/"
)

stops_df = read_gtfs("stops.txt")
stops_df.select(
    stops_schema.fieldNames()) \
    .write.mode('overwrite') \
    .parquet(f"{S3_CURATED}/stops/"
)

trips_df = read_gtfs("trips.txt")
trips_df.select(
    trips_schema.fieldNames()) \
    .write.mode('overwrite') \
    .parquet(f"{S3_CURATED}/trips/"
)

calendar_df = read_gtfs("calendar.txt")
calendar_df = calendar_df \
    .withColumn("start_date", to_date(col("start_date"), "yyyyMMdd")) \
    .withColumn("end_date", to_date(col("end_date"), "yyyyMMdd"))

calendar_df = read_gtfs("calendar.txt")
calendar_df = calendar_df \
    .withColumn("start_date", to_date(col("start_date"), "yyyyMMdd")) \
    .withColumn("end_date", to_date(col("end_date"), "yyyyMMdd"))

calendar_df.select(calendar_schema.fieldNames()) \
    .write.mode("overwrite") \
    .partitionBy("ingestion_date") \
    .parquet(f"{S3_CURATED}/calendar")

spark.stop()
