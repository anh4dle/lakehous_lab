from datetime import datetime, timedelta, timezone
import uuid
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_timestamp

def get_current_utc_string() -> str:
    """Returns a deterministic UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def read_new_data(spark_client, base_path: str, last_processed_date: datetime, pipeline_name: str) -> DataFrame:
    """Read all data from a path into a DataFrame deterministically."""
    dates = (
        pd.date_range(
            start=last_processed_date + timedelta(days=1),
            end=last_processed_date + timedelta(days=1),
        )
        .strftime("%Y-%m-%d")
        .tolist()
    )
    if not dates:
        return spark_client.spark.createDataFrame([], schema="temp_schema")

    paths = [f"{base_path}/dt={d}/" for d in dates]
    print(f"Reading new data from {paths}")
    
    df_raw = (
        spark_client.spark.read.option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(paths)
    )
    
    # Static execution timestamp computed once in Python
    batch_time = get_current_utc_string()

    df = (
        df_raw.withColumn("pipeline_name", lit(pipeline_name))
        .withColumn(
            "event_time",
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .withColumn("_ingested_at", to_timestamp(lit(batch_time)))  # Deterministic literal
    )
    return df

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
