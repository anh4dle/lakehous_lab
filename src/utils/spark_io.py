from datetime import datetime, timedelta, timezone
import uuid
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_timestamp
from src.config.config import BASE_INPUT_PATH


def get_current_utc_string() -> str:
    """Returns a deterministic UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def read_new_data(
    spark_client,
    source: str,
    last_processed_date: datetime,
    pipeline_name: str,
    source_type="iceberg",
) -> DataFrame:
    """Read the next unprocessed day's data from Raw or Bronze."""

    next_date = (last_processed_date + timedelta(days=1)).strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # RAW: JSON files in S3
    # ------------------------------------------------------------------

    if source_type == "raw":
        path = f"{source}/dt={next_date}/"

        print(f"Reading new data from {path}")
        print("Input format: json")

        df_raw = (
            spark_client.spark.read.option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .json(path)
        )

    # ------------------------------------------------------------------
    # BRONZE/SILVER/GOLD: Iceberg table
    # ------------------------------------------------------------------

    elif source_type == "iceberg":
        print(f"Reading new data from Iceberg table: {source}")
        print(f"Processing date: {next_date}")
        print("Input format: iceberg")

        df_raw = spark_client.spark.table(source).filter(
            col("event_time").cast("date") == lit(next_date).cast("date")
        )

    else:
        raise ValueError(
            f"Unsupported source_type: {source_type}. Expected 'raw' or 'iceberg'."
        )

    # ------------------------------------------------------------------
    # Add pipeline metadata
    # ------------------------------------------------------------------

    batch_time = get_current_utc_string()

    return (
        df_raw.withColumn(
            "pipeline_name",
            lit(pipeline_name),
        )
        .withColumn(
            "event_time",
            to_timestamp(
                col("event_time"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn(
            "_ingested_at",
            to_timestamp(lit(batch_time)),
        )
    )


def write_to_iceberg(df: DataFrame, table_name: str):
    """Executes an idempotent MERGE INTO without cache overhead."""
    # Remove duplicate events within this batch
    df = df.dropDuplicates(["event_id"])

    # Register as a temporary view
    temp_view_name = f"staging_{uuid.uuid4().hex[:8]}"
    df.createOrReplaceTempView(temp_view_name)
    # Idempotent upsert
    try:
        df.sparkSession.sql(f"""
            MERGE INTO {table_name} AS target
            USING {temp_view_name} AS source
            ON target.event_id = source.event_id
            WHEN MATCHED THEN
              UPDATE SET *
            WHEN NOT MATCHED THEN
              INSERT *
        """)
    finally:
        df.sparkSession.catalog.dropTempView(temp_view_name)
