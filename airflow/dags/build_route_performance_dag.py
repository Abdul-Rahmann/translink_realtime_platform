from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

SPARK_APP_PATH = "/opt/spark-apps/batch/build_route_performance.py"

with DAG(
    dag_id="build_route_performance",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["spark", "analytics", "routes", "translink"],
) as dag:

    build_route_performance = SparkSubmitOperator(
        task_id="spark_build_route_performance",
        application=SPARK_APP_PATH,
        conn_id="spark_default",
        verbose=True,

        jars='/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar',

        conf={
            "spark.master": "local[*]",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        
        env_vars={
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-west-2")
        }
    )

    build_route_performance