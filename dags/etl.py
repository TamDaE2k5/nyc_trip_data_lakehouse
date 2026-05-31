import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Spark env configurations
SPARK_ENV = {
    'CATALOG_URI': 'http://iceberg-rest:8181',
    'S3_ENDPOINT': 'http://minio:9000',
    'S3_ACCESS_KEY': 'minioadmin',
    'S3_SECRET_KEY': 'minioadmin',
    'DATA_DIR': '/app/data',
    'AWS_REGION': 'us-east-1'
}

# Image name of the Spark container
SPARK_IMAGE = os.environ.get("SPARK_IMAGE_NAME", "lakehouse-spark:latest")

# Host project path for mounting folders (must reflect the host path for Docker daemon)
HOST_PATH = os.environ.get("HOST_PROJECT_PATH", "D:/Source code/taxi_lakehouse")

def create_spark_task(task_id, script_path, dag):
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=f"/opt/spark/bin/spark-submit --driver-memory 2g --master local[*] /app/spark_jobs/{script_path}",
        api_version='auto',
        auto_remove=True,
        network_mode='lakehouse-net',
        mounts=[
            Mount(source=f"{HOST_PATH}/spark_jobs", target="/app/spark_jobs", type="bind"),
            Mount(source=f"{HOST_PATH}/data", target="/app/data", type="bind")
        ],
        environment=SPARK_ENV,
        mount_tmp_dir=False,  # CRITICAL for Windows/WSL Compatibility
        dag=dag
    )

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='NYC Taxi Trip Lakehouse Monthly ETL Pipeline',
    schedule_interval='0 0 5 * *',  # Day 5 of every month
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['lakehouse'],
) as dag:
    init_tables = create_spark_task('init_tables', 'scripts/init_tables.py', dag)

    ingest_green = create_spark_task('ingest_green', 'bronze/in_green.py', dag)
    ingest_yellow = create_spark_task('ingest_yellow', 'bronze/in_yellow.py', dag)

    init_tables >> ingest_green
    init_tables >> ingest_yellow