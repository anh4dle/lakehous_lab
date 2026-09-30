from datetime import datetime, timedelta
from src.transform.transform_script import transform_to_silver_fact_orders
from src.client.spark_client import SparkClient
from src.config.config import (
    BRONZE_ORDER_TABLE,
    ORDER_SILVER_PIPELINE_NAME,
    SILVER_ORDER_TABLE,
)
from src.utils.registry import (
    get_last_processed_date,
    update_registry_table,
)
from src.utils.spark_io import (
    read_new_data,
    write_to_iceberg,
)
from src.utils.validation import validate_schema

# =========================================================
"""
Get last processed date from registry table
Read data from S3 path
Transform data to Bronze schema
Write data to Bronze table
"""


# ========================= MAIN FUNCTION =========================
def ingest_to_silver():
    spark = SparkClient()
    last_processed_date = get_last_processed_date(spark, ORDER_SILVER_PIPELINE_NAME)
    print(f"Last processed date: {last_processed_date}")
    last_processed_date_dt = datetime.strptime(last_processed_date, "%Y-%m-%d")
    # Read new data based on checkpoint value
    df = read_new_data(
        spark,
        BRONZE_ORDER_TABLE,
        last_processed_date_dt,
        pipeline_name=ORDER_SILVER_PIPELINE_NAME,
    )

    # If there are new record then start the ingestion
    new_count = df.count()
    print(f"Found {new_count} new records to ingest.")

    if new_count > 0:
        df = transform_to_silver_fact_orders(df)
        validate_schema(
            spark_client=spark, source_df=df, target_table_name=SILVER_ORDER_TABLE
        )
        write_to_iceberg(df, SILVER_ORDER_TABLE)

        update_registry_table(
            spark,
            (last_processed_date_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
            ORDER_SILVER_PIPELINE_NAME,
        )

        print(f"Successfully loaded {new_count} records into Iceberg.")
    else:
        print("No new data to process.")

    print("=== Pipeline raw to bronze finished ===")


if __name__ == "__main__":
    ingest_to_silver()
