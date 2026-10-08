from datetime import datetime
from src.config.config import (
    REGISTRY_TABLE,
)


def get_last_processed_date(spark_client, pipeline_name) -> str:
    """Retrieve last successful processed date"""
    df = spark_client.spark.sql(
        f"""
        SELECT COALESCE(MAX(last_processed_date), '2026-05-01') as last_date 
        FROM {REGISTRY_TABLE}
        WHERE pipeline_name = '{pipeline_name}'
    """
    )
    return df.collect()[0]["last_date"]


def update_registry_table(spark_client, max_date: str, pipeline_name):
    """Record successful load"""
    control_df = spark_client.spark.createDataFrame(
        [(pipeline_name, max_date, datetime.now(), "SUCCESS")],
        ["pipeline_name", "last_processed_date", "load_timestamp", "status"],
    )
    control_df.writeTo(REGISTRY_TABLE).append()
