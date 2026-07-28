from datetime import datetime, timedelta
from src.transform.transform_script import transform_to_bronze
from src.client.spark_client import SparkClient
from src.config.config import (
    BASE_INPUT_PATH,
    TABLE_FULL_NAME,
    CUSTOMER_BRONZE_PIPELINE_NAME
)
from src.utils.registry import (
    get_last_processed_date,
    read_new_data,
    write_to_iceberg,
    update_registry_table,
)
# =========================================================
"""
Get last processed date from registry table
Read data from S3 path
Transform data to Bronze schema
Write data to Bronze table
"""

def validate_schema(spark_client, df, table_name):
    """Check if the schema of the DataFrame matches
    the target Iceberg table schema
    iceberg.bronze.fact_customer_events
    """
    target_schema = spark_client.spark.table(table_name).schema
    print("Showing target schema:", target_schema)
    print("Showing source schema:")
    df.printSchema()
    for source_field, target_field in zip(df.schema, target_schema):
        assert source_field.name == target_field.name
        assert source_field.dataType == target_field.dataType

# ========================= MAIN FUNCTION =========================
def main():
    spark = SparkClient()
    CUSTOMER_BRONZE_PIPELINE_NAME
    last_processed_date = get_last_processed_date(spark, CUSTOMER_BRONZE_PIPELINE_NAME)
    print(f"Last processed date: {last_processed_date}")
    last_processed_date_dt = datetime.strptime(last_processed_date, "%Y-%m-%d")
    # Read new data based on checkpoint value
    df = read_new_data(spark, BASE_INPUT_PATH, last_processed_date_dt, CUSTOMER_BRONZE_PIPELINE_NAME)

    # If there are new record then start the ingestion
    new_count = df.count()
    print(f"Found {new_count} new records to ingest.")

    if new_count > 0:
        df = transform_to_bronze(df)
        validate_schema(spark_client=spark, df=df, table_name=TABLE_FULL_NAME)
        write_to_iceberg(df, TABLE_FULL_NAME)

        update_registry_table(
            spark, (last_processed_date_dt + timedelta(days=1)).strftime("%Y-%m-%d")
        )

        print(f"Successfully loaded {new_count} records into Iceberg.")
    else:
        print("No new data to process.")

    print("=== Pipeline raw to bronze finished ===")


if __name__ == "__main__":
    main()
