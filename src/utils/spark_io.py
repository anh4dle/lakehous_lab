from pyspark.sql import DataFrame
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
)
from pyspark.sql.functions import lit
import uuid
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
    temp_view_name = f"staging_{uuid.uuid4().hex[:8]}"

    df.createOrReplaceTempView(temp_view_name)
    # Idempotent upsert
    df.sparkSession.sql(f"""
        MERGE INTO {table_name} AS target
        USING staging_events AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
    """)