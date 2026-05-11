from datetime import datetime, timedelta

from airflow.decorators import dag, task

from utils.ecobici_utils import (
    extract_station_status_to_bronze_parquet,
    publish_ecobici_bronze_to_trino,
)


BUCKET_NAME = "datalake"


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="ecobici_station_status_bronze_pipeline",
    description="Ingesta Ecobici Bronze Parquet y publicación en Trino",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecobici", "bronze", "minio", "trino", "parquet"],
)
def ecobici_station_status_bronze_pipeline():

    @task(task_id="extract_station_status_to_bronze")
    def extract_bronze():
        return extract_station_status_to_bronze_parquet(
            bucket_name=BUCKET_NAME
        )

    @task(task_id="publish_bronze_to_trino")
    def publish_to_trino():
        publish_ecobici_bronze_to_trino(
            bucket_name=BUCKET_NAME
        )

    bronze_file = extract_bronze()
    bronze_file >> publish_to_trino()


ecobici_station_status_bronze_pipeline()