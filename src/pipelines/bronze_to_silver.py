from datetime import datetime, timedelta
from src.transform.transform_script import transform_to_silver_dim_customer
from spark.spark_client import SparkClient
from src.config.config import (
    BASE_INPUT_PATH,
    TABLE_FULL_NAME,
    CUSTOMER_SILVER_PIPELINE_NAME
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
    last_processed_date = get_last_processed_date(spark, CUSTOMER_SILVER_PIPELINE_NAME)
    print(f"Last processed date: {last_processed_date}")
    last_processed_date_dt = datetime.strptime(last_processed_date, "%Y-%m-%d", CUSTOMER_SILVER_PIPELINE_NAME)
    # Read new data based on checkpoint value
    df = read_new_data(spark, BASE_INPUT_PATH, last_processed_date_dt, CUSTOMER_SILVER_PIPELINE_NAME)

    # If there are new record then start the ingestion
    new_count = df.count()
    print(f"Found {new_count} new records to ingest.")

    if new_count > 0:
        df = transform_to_silver_dim_customer(df)
        validate_schema(spark_client=spark, df=df, table_name=TABLE_FULL_NAME)
        write_to_iceberg(df, TABLE_FULL_NAME)

        update_registry_table(
            spark, (last_processed_date_dt + timedelta(days=1)).strftime("%Y-%m-%d"), CUSTOMER_SILVER_PIPELINE_NAME
        )

        print(f"Successfully loaded {new_count} records into Iceberg.")
    else:
        print("No new data to process.")

    print("=== Pipeline raw to bronze finished ===")


if __name__ == "__main__":
    ingest_to_silver()
