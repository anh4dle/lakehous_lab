from pyspark.sql import DataFrame
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
)
from src.config.config import (
    REGISTRY_TABLE,
)
from pyspark.sql.functions import lit


def get_last_processed_date(spark_client, pipeline_name) -> str:
    """Retrieve last successful processed date"""
    df = spark_client.spark.sql(
        f"""
        SELECT COALESCE(MAX(last_processed_date), '2026-05-01') as last_date 
        FROM {REGISTRY_TABLE}
        WHERE pipeline_name = {pipeline_name}
    """
    )
    return df.collect()[0]["last_date"]

def read_new_data(spark_client, base_path: str, last_processed_date: datetime, pipeline_name):
    """Read all data from a path into a DataFrame"""
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
    #Pipeline name is customer_raw_to_bronze
    df = (
        df_raw.withColumn("pipeline_name", lit(pipeline_name))
        .withColumn(
            "event_time",
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .withColumn("_ingested_at", current_timestamp())
    )

    return df

def write_to_iceberg(df: DataFrame, table_name: str):
    # Remove duplicate events within this batch
    df = df.dropDuplicates(["event_id"])
    # Register as a temporary view
    df.createOrReplaceTempView("staging_events")
    # Idempotent upsert
    df.sparkSession.sql(f"""
        MERGE INTO {table_name} AS target
        USING staging_events AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN
          INSERT *
    """)

def update_registry_table(spark_client, max_date: str, pipeline_name):
    """Record successful load"""
    control_df = spark_client.spark.createDataFrame(
        [(pipeline_name, max_date, datetime.now(), "SUCCESS")],
        ["pipeline_name", "last_processed_date", "load_timestamp", "status"],
    )
    control_df.writeTo(REGISTRY_TABLE).append()