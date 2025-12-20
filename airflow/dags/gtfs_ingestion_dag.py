from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import requests, zipfile, os, boto3
from pathlib import Path

GTFS_URL = "https://gtfs-static.translink.ca/gtfs/google_transit.zip"
LOCAL_TMP_DIR = "/tmp/gtfs"
S3_BUCKET = "translink-d"
S3_PREFIX = "raw/gtfs"

GTFS_FILES = [
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "calendar.txt"
]

def download_gtfs(**context):
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    zip_path = f"{LOCAL_TMP_DIR}/gtfs.zip"

    response = requests.get(GTFS_URL)
    response.raise_for_status()

    with open(zip_path, "wb") as f:
        f.write(response.content)

    return zip_path

def extract_gtfs(**context):
    zip_path = f"{LOCAL_TMP_DIR}/gtfs.zip"
    extract_path = f"{LOCAL_TMP_DIR}/extracted"
    os.makedirs(extract_path, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_path)

    return extract_path

def upload_to_s3(**context):
    execution_date = context['ds']
    extraction_path = f"{LOCAL_TMP_DIR}/extracted"

    s3 = boto3.client('s3')

    for file_name in GTFS_FILES:
        file_path = Path(extraction_path) / file_name
        if not file_path.exists():
            continue

        s3_key = (
            f"{S3_PREFIX}/ingestion_date={execution_date}/{file_name}"
        )

        s3.upload_file(
            Filename=str(file_path),
            Bucket=S3_BUCKET,
            Key=s3_key
        )

with DAG(
    dag_id="gtfs_static_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["gtfs", "batch", "translink"]
) as dag:

    download_task = PythonOperator(
        task_id="download_gtfs_zip",
        python_callable=download_gtfs
    )

    extract_task = PythonOperator(
        task_id="extract_gtfs_files",
        python_callable=extract_gtfs
    )

    upload_task = PythonOperator(
        task_id="upload_gtfs_to_s3",
        python_callable=upload_to_s3
    )

    spark_transform_task = SparkSubmitOperator(
        task_id='spark_transform_gtfs',
        application='/opt/spark-apps/batch/gtfs_transform.py',
        name='gtfs_transform_job',
        jars='/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar',

        verbose=True,
        conf={
            "spark.master": "local[*]",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        },
        application_args=["--execution_date", "{{ ds }}"]
    )
    


    download_task >> extract_task >> upload_task >> spark_transform_task