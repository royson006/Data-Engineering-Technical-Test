import json
from datetime import datetime, timezone
from io import BytesIO

import boto3
import polars as pl
import requests
import trino

from botocore.exceptions import ClientError


GBFS_URL = "https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json"

TRINO_HOST = "coordinator"
TRINO_PORT = 8080
TRINO_USER = "airflow"

TRINO_CATALOG = "bronze"
TRINO_SCHEMA = "prueba"
TRINO_TABLE = "ecobici_station_status"

LAKEHOUSE_LAYER = "bronze"
DATASET_NAME = "ecobici/station_status"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio1234"
    )


def get_trino_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def ensure_bucket_exists(
    s3_client,
    bucket_name: str,
) -> None:

    try:
        s3_client.head_bucket(
            Bucket=bucket_name,
        )

    except ClientError:
        s3_client.create_bucket(
            Bucket=bucket_name,
        )


def object_exists(
    s3_client,
    bucket_name: str,
    object_name: str,
) -> bool:

    try:
        s3_client.head_object(
            Bucket=bucket_name,
            Key=object_name,
        )

        return True

    except ClientError as error:

        status_code = error.response[
            "ResponseMetadata"
        ]["HTTPStatusCode"]

        if status_code == 404:
            return False

        raise


def get_station_status_url() -> str:

    response = requests.get(
        GBFS_URL,
        timeout=30,
    )

    response.raise_for_status()

    feeds = response.json()["data"]["en"]["feeds"]

    for feed in feeds:

        if feed["name"] == "station_status":
            return feed["url"]

    raise ValueError(
        "station_status feed not found"
    )


def read_existing_parquet_from_s3(
    s3_client,
    bucket_name: str,
    object_name: str,
) -> pl.DataFrame:

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_name,
    )

    data = response["Body"].read()

    return pl.read_parquet(
        BytesIO(data)
    )


def write_parquet_to_s3(
    s3_client,
    bucket_name: str,
    object_name: str,
    df: pl.DataFrame,
) -> None:

    buffer = BytesIO()

    df.write_parquet(buffer)

    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_name,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )


def execute_trino_query(
    query: str,
) -> None:

    conn = get_trino_connection()

    cur = conn.cursor()

    cur.execute(query)

    cur.fetchall()

    cur.close()
    conn.close()


def publish_ecobici_bronze_to_trino(
    bucket_name: str,
) -> None:

    create_schema_sql = f"""
    CREATE SCHEMA IF NOT EXISTS
    {TRINO_CATALOG}.{TRINO_SCHEMA}
    WITH (
        location = 's3a://{bucket_name}/{LAKEHOUSE_LAYER}/{TRINO_SCHEMA}/'
    )
    """

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS
    {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}
    (
        station_id VARCHAR,
        num_bikes_available BIGINT,
        num_docks_available BIGINT,
        is_installed BIGINT,
        is_renting BIGINT,
        is_returning BIGINT,
        last_reported_unix BIGINT,
        last_reported TIMESTAMP,
        source_system VARCHAR,
        batch_id VARCHAR,
        ingestion_timestamp VARCHAR,
        ingestion_date VARCHAR,
        ingestion_hour VARCHAR
    )
    WITH (
        external_location =
        's3a://{bucket_name}/{LAKEHOUSE_LAYER}/{DATASET_NAME}/',

        format = 'PARQUET',

        partitioned_by = ARRAY[
            'ingestion_date',
            'ingestion_hour'
        ]
    )
    """

    sync_partitions_sql = f"""
    CALL {TRINO_CATALOG}.system.sync_partition_metadata(
        schema_name => '{TRINO_SCHEMA}',
        table_name => '{TRINO_TABLE}',
        mode => 'FULL'
    )
    """

    execute_trino_query(
        create_schema_sql
    )

    execute_trino_query(
        create_table_sql
    )

    execute_trino_query(
        sync_partitions_sql
    )


def extract_station_status_to_bronze_parquet(
    bucket_name: str,
) -> str:

    s3_client = get_s3_client()

    ensure_bucket_exists(
        s3_client=s3_client,
        bucket_name=bucket_name,
    )

    station_status_url = (
        get_station_status_url()
    )

    response = requests.get(
        station_status_url,
        timeout=30,
    )

    response.raise_for_status()

    stations = response.json()[
        "data"
    ]["stations"]

    now = datetime.now(
        timezone.utc
    )

    ingestion_date = now.strftime(
        "%Y-%m-%d"
    )

    ingestion_hour = now.strftime(
        "%H"
    )

    ingestion_timestamp = (
        now.isoformat()
    )

    batch_id = now.strftime(
        "%Y%m%d_%H%M%S"
    )

    parquet_object_name = (
        f"{LAKEHOUSE_LAYER}/"
        f"{DATASET_NAME}/"
        f"ingestion_date={ingestion_date}/"
        f"ingestion_hour={ingestion_hour}/"
        f"station_status.parquet"
    )

    new_df = (
        pl.DataFrame(stations)

        .select(
            [
                pl.col("station_id")
                .cast(pl.Utf8),

                pl.col(
                    "num_bikes_available"
                )
                .cast(pl.Int64),

                pl.col(
                    "num_docks_available"
                )
                .cast(pl.Int64),

                pl.col(
                    "is_installed"
                )
                .cast(pl.Int64),

                pl.col(
                    "is_renting"
                )
                .cast(pl.Int64),

                pl.col(
                    "is_returning"
                )
                .cast(pl.Int64),

                pl.col(
                    "last_reported"
                )
                .cast(pl.Int64)
                .alias(
                    "last_reported_unix"
                ),
            ]
        )

        .drop_nulls(
            subset=[
                "station_id",
                "num_bikes_available",
                "num_docks_available",
                "last_reported_unix",
            ]
        )

        .filter(
            pl.col(
                "num_bikes_available"
            ) >= 0
        )

        .filter(
            pl.col(
                "num_docks_available"
            ) >= 0
        )

        .with_columns(
            [
                pl.from_epoch(
                    "last_reported_unix",
                    time_unit="s",
                ).alias(
                    "last_reported"
                ),

                pl.lit("ecobici")
                .alias(
                    "source_system"
                ),

                pl.lit(batch_id)
                .alias("batch_id"),

                pl.lit(
                    ingestion_timestamp
                ).alias(
                    "ingestion_timestamp"
                ),

                pl.lit(
                    ingestion_date
                ).alias(
                    "ingestion_date"
                ),

                pl.lit(
                    ingestion_hour
                ).alias(
                    "ingestion_hour"
                ),
            ]
        )

        .unique(
            subset=[
                "station_id",
                "last_reported",
            ]
        )
    )

    if object_exists(
        s3_client=s3_client,
        bucket_name=bucket_name,
        object_name=parquet_object_name,
    ):

        existing_df = (
            read_existing_parquet_from_s3(
                s3_client=s3_client,
                bucket_name=bucket_name,
                object_name=parquet_object_name,
            )
        )

        final_df = pl.concat(
            [existing_df, new_df],
            how="vertical",
        )

    else:
        final_df = new_df

    write_parquet_to_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        object_name=parquet_object_name,
        df=final_df,
    )

    publish_ecobici_bronze_to_trino(
        bucket_name=bucket_name,
    )

    return parquet_object_name