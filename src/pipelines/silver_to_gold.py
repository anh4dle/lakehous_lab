from datetime import datetime, timedelta
from src.transform.transform_script import transform_to_gold_daily_orders_by_country
from src.client.spark_client import SparkClient
from src.config.config import (
    BASE_INPUT_PATH,
    GOLD_PIPELINE_NAME,
    GOLD_TABLE,
    SILVER_ORDER_TABLE,
    SILVER_CUSTOMER_TABLE,
)
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
    df_order = read_new_data(
        spark, SILVER_ORDER_TABLE, last_processed_date_dt, GOLD_PIPELINE_NAME
    )
    df_customer = read_new_data(
        spark, SILVER_CUSTOMER_TABLE, last_processed_date_dt, GOLD_PIPELINE_NAME
    )

    # If there are new record then start the ingestion
    order_new_count = df_order.count()
    customer_new_count = df_order.count()
    print(f"Found {order_new_count} new order records to ingest.")
    print(f"Found {customer_new_count} new customerrecords to ingest.")

    if order_new_count > 0 and customer_new_count > 0:
        df = transform_to_gold_daily_orders_by_country(df_order, df_customer)
        write_to_iceberg(df, GOLD_TABLE, primary_keys=["report_date", "country"])

        update_registry_table(
            spark, (last_processed_date_dt + timedelta(days=1)).strftime("%Y-%m-%d"),\
                pipeline_name=GOLD_PIPELINE_NAME
        )

        print("Successfully loaded new records into Iceberg.")
    else:
        print("No new data to process.")

    print("=== Pipeline raw to bronze finished ===")


if __name__ == "__main__":
    ingest_to_gold()
