from datetime import datetime, timedelta
import pandas as pd  
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name,
    regexp_extract,
    col,
    current_timestamp,
    to_timestamp,
)
from datetime import datetime
from spark.spark_client import SparkClient
from pyspark.sql.functions import lit

# ========================= CONFIG =========================
BASE_INPUT_PATH = (
    "s3a://warehouse/raw/customers"  # ← Change if bucket name is different
)
TABLE_FULL_NAME = "iceberg.bronze.fact_customer_events"
REGISTRY_TABLE = "iceberg.metadata.ingestion_registry"
# =========================================================


def get_last_processed_date(spark_client) -> str:
    """Retrieve last successful processed date"""
    df = spark_client.spark.sql(
        f"""
        SELECT COALESCE(MAX(processed_date), '1900-01-01') as last_date 
        FROM {REGISTRY_TABLE}
        WHERE pipeline_name = 'customer_raw_to_bronze'
    """
    )
    return df.collect()[0]["last_date"]


def read_new_data(spark, base_path: str, last_processed_date: str):
    """Read all data from a path into a DataFrame"""
    start_date = datetime.strptime(last_processed_date, "%Y-%m-%d")
    dates = (
        pd.date_range(start=start_date + timedelta(days=1), end=datetime.today())
        .strftime("%Y-%m-%d")
        .tolist()
    )

    if not dates:
        return spark.createDataFrame([], schema="temp_schema")

    paths = [f"{base_path}/dt={d}/customers_*.json" for d in dates]

    df_raw = (
        spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(paths)
    )

    # Add ingestion metadata
    df = (
        df_raw.withColumn("pipeline_name", "customer_raw_to_bronze")
        .withColumn(
            "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss")
        )
        .withColumn("_ingested_at", current_timestamp())
    )

    return df


def validate_schema(spark, df, table_name):
    target_schema = spark.table(table_name).schema

    if df.schema != target_schema:
        raise Exception("Schema mismatch detected.")


def write_to_iceberg(df, table_name):
    """Append to Iceberg table"""

    (
        df.writeTo(table_name)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .option("merge-schema", "true")  # Schema merging when there's a drift
        .append()
    )


def update_registry_table(spark, max_date: str, record_count: int):
    """Record successful load"""
    control_df = spark.createDataFrame(
        [("customer_raw_to_bronze", max_date, datetime.now(), "SUCCESS")],
        ["pipeline_name", "processed_date", "load_timestamp", "status"],
    )
    control_df.writeTo(REGISTRY_TABLE).append()


# ========================= MAIN FUNCTION =========================
def main():
    spark = SparkClient()
    last_date = get_last_processed_date(spark)
    print(f"Last processed date: {last_date}")
    # Read new data based on checkpoint value
    df = read_new_data(spark, BASE_INPUT_PATH, last_date)

    # If there are new record then start the ingestion
    new_count = df.count()
    print(f"Found {new_count} new records to ingest.")

    if new_count > 0:
        validate_schema(spark, df, TABLE_FULL_NAME)
        write_to_iceberg(df, TABLE_FULL_NAME)

        max_date = df.selectExpr("max(dt_folder)").collect()[0][0]
        update_registry_table(spark, max_date, new_count)

        print(f"Successfully loaded {new_count} records into Iceberg.")
    else:
        print("No new data to process.")

    print("=== Incremental load finished ===")


if __name__ == "__main__":
    main()
