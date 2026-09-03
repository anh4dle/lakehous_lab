from datetime import datetime, timedelta
from src.transform.transform_script import transform_to_gold_daily_orders_by_country
from src.client.spark_client import SparkClient
from src.config.config import BASE_INPUT_PATH, TABLE_FULL_NAME, GOLD_PIPELINE_NAME
from src.utils.registry import (
    get_last_processed_date,
    update_registry_table,
)
from src.utils.spark_io import (
    read_new_data,
    write_to_iceberg,
)

# =========================================================
"""
Get last processed date from registry table
Read data from S3 path
Transform data to Bronze schema
Write data to Bronze table
"""


# ========================= MAIN FUNCTION =========================
def ingest_to_gold():
    spark = SparkClient()
    last_processed_date = get_last_processed_date(spark, GOLD_PIPELINE_NAME)
    print(f"Last processed date: {last_processed_date}")
    last_processed_date_dt = datetime.strptime(last_processed_date, "%Y-%m-%d")
    # Read new data based on checkpoint value
    df = read_new_data(
        spark, BASE_INPUT_PATH, last_processed_date_dt, GOLD_PIPELINE_NAME
    )

    # If there are new record then start the ingestion
    new_count = df.count()
    print(f"Found {new_count} new records to ingest.")

    if new_count > 0:
        df = transform_to_gold_daily_orders_by_country(df)
        write_to_iceberg(df, TABLE_FULL_NAME)

        update_registry_table(
            spark, (last_processed_date_dt + timedelta(days=1)).strftime("%Y-%m-%d")
        )

        print(f"Successfully loaded {new_count} records into Iceberg.")
    else:
        print("No new data to process.")

    print("=== Pipeline raw to bronze finished ===")


if __name__ == "__main__":
    ingest_to_gold()
