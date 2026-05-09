from __future__ import annotations

import io
import os
from datetime import datetime

import boto3
import polars as pl
import trino
from airflow.decorators import dag, task


DAG_ID = "etl_engineer_challenge"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio1234")

LANDING_BUCKET = "bck-landing"
LANDING_KEY = "data/data_prueba_tecnica.csv"

BRONZE_BUCKET = "bck-bronze"
BRONZE_KEY = "master/data_prueba_tecnica.parquet"

TRINO_HOST = os.getenv("TRINO_HOST", "coordinator")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "airflow", "minio", "trino"],
)
def etl_engineer_challenge():

    # @task
    # def create_buckets() -> None:
    #     client = s3_client()
    #     for bucket in [LANDING_BUCKET, BRONZE_BUCKET]:
    #         existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    #         if bucket not in existing:
    #             client.create_bucket(Bucket=bucket)

    # @task
    # def upload_landing_file() -> str:
    #     client = s3_client()
    #     client.upload_file(LOCAL_INPUT_FILE, LANDING_BUCKET, LANDING_KEY)
    #     return f"s3://{LANDING_BUCKET}/{LANDING_KEY}"


    @task(task_id="read_csv_from_minio")
    def read_csv_from_minio() -> str:
        """
        3.1 Lectura de datos:
        Lee el archivo CSV previamente cargado en MinIO.
        """
        s3_client = get_s3_client()

        response = s3_client.get_object(
            Bucket=LANDING_BUCKET,
            Key=LANDING_KEY,
        )

        csv_content = response["Body"].read().decode("utf-8")
        return csv_content

    @task(task_id="clean_transform_and_aggregate")
    def clean_transform_and_aggregate(csv_content: str) -> str:
        """
        3.2 Limpieza de datos, transformación y agregaciones:
        - Corrige inconsistencias.
        - Normaliza name y company_id.
        - Convierte created_at a fecha.
        - Filtra ids nulos.
        - Realiza agregaciones sobre name y created_at.
        """
        df = pl.read_csv(io.StringIO(csv_content), infer_schema_length=1000)

        df = df.with_columns(
            [
                pl.col("id").cast(pl.Utf8).str.strip_chars().alias("id"),
                pl.col("name").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("name"),
                pl.col("company_id").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("company_id"),
                pl.col("created_at")
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.strptime(pl.Datetime, strict=False)
                .alias("created_at"),
            ]
        )

        if "amount" in df.columns:
            df = df.with_columns(
                pl.col("amount").cast(pl.Float64, strict=False).alias("amount")
            )

        df_clean = df.filter(
            pl.col("id").is_not_null()
            & (pl.col("id") != "")
            & pl.col("created_at").is_not_null()
        )

        aggregation_expressions = [
            pl.len().alias("transaction_count"),
        ]

        if "amount" in df_clean.columns:
            aggregation_expressions.extend(
                [
                    pl.col("amount").sum().alias("total_amount"),
                    pl.col("amount").mean().alias("avg_amount"),
                    pl.col("amount").min().alias("min_amount"),
                    pl.col("amount").max().alias("max_amount"),
                ]
            )

        df_aggregated = (
            df_clean
            .with_columns(pl.col("created_at").dt.date().alias("created_date"))
            .group_by(["name", "created_date"])
            .agg(aggregation_expressions)
            .sort(["created_date", "name"])
        )

        return df_aggregated.write_json()

    @task(task_id="save_parquet_to_bronze")
    def save_parquet_to_bronze(df_json: str) -> None:
        """
        3.3 Guardado del resultado:
        Guarda el resultado procesado en formato parquet en MinIO.
        """
        df = pl.read_json(io.StringIO(df_json))

        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)

        s3_client = get_s3_client()

        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=BRONZE_KEY,
            Body=buffer.getvalue(),
        )

    @task(task_id="enable_trino_table")
    def enable_trino_table() -> None:
        """
        4.1 Trino:
        Crea schema y tabla externa para consultar el parquet desde Trino.
        """
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog="bronze",
            schema="prueba",
        )

        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE SCHEMA IF NOT EXISTS bronze.prueba
            WITH (location = 's3a://bck-bronze/prueba.db/')
            """
        )

        cursor.execute("DROP TABLE IF EXISTS bronze.prueba.tbl_data")

        cursor.execute(
            """
            CREATE TABLE bronze.prueba.tbl_data (
                name VARCHAR,
                created_date VARCHAR,
                transaction_count BIGINT,
                total_amount DOUBLE,
                avg_amount DOUBLE,
                min_amount DOUBLE,
                max_amount DOUBLE
            )
            WITH (
                external_location = 's3a://bck-bronze/master/',
                format = 'PARQUET'
            )
            """
        )

    csv_data = read_csv_from_minio()
    transformed_data = clean_transform_and_aggregate(csv_data)
    parquet_file = save_parquet_to_bronze(transformed_data)
    trino_table = enable_trino_table()

    csv_data >> transformed_data >> parquet_file >> trino_table


etl_engineer_challenge()
