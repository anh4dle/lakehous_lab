from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import (
    input_file_name,
    col,
    current_timestamp,
    to_timestamp,
    struct,
)
from pyspark.sql import DataFrame
from spark.spark_client import SparkClient
from pyspark.sql.functions import lit

BASE_INPUT_PATH = "s3a://warehouse/raw/customers"
TABLE_FULL_NAME = "iceberg.bronze.fact_customer_events"
REGISTRY_TABLE = "iceberg.metadata.ingestion_registry"
# =========================================================


def get_last_processed_date(spark_client) -> str:
    """Retrieve last successful processed date"""
    df = spark_client.spark.sql(
        f"""
        SELECT COALESCE(MAX(processed_date), '2026-05-01') as last_date 
        FROM {REGISTRY_TABLE}
        WHERE pipeline_name = 'customer_raw_to_bronze'
    """
    )
    return df.collect()[0]["last_date"]


def read_new_data(spark_client, base_path: str, last_processed_date: datetime):
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
        df_raw.withColumn("pipeline_name", lit("customer_raw_to_bronze"))
        .withColumn(
            "event_time",
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .withColumn("_ingested_at", current_timestamp())
    )

    return df


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


def write_to_iceberg(df, table_name):
    """Append to Iceberg table"""

    (
        df.writeTo(table_name)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .option("merge-schema", "true")  # Schema merging when there's a drift
        .append()
    )


def update_registry_table(spark_client, max_date: str):
    """Record successful load"""
    control_df = spark_client.spark.createDataFrame(
        [("customer_raw_to_bronze", max_date, datetime.now(), "SUCCESS")],
        ["pipeline_name", "processed_date", "load_timestamp", "status"],
    )
    control_df.writeTo(REGISTRY_TABLE).append()


def transform_to_bronze(df: DataFrame) -> DataFrame:
    """
    Transform raw customer events into the Bronze schema.
    """
    return (
        df.withColumn(
            "customer",
            struct(
                col("customer.customer_id").alias("customer_id"),
                col("customer.name").alias("name"),
                col("customer.email").alias("email"),
                col("customer.country").alias("country"),
                to_timestamp(col("customer.created_at")).alias("created_at"),
            ),
        )
        .withColumn("_source_file", input_file_name())
        .drop("pipeline_name")
        .select(
            "event_id",
            "event_time",
            "customer",
            "_ingested_at",
            "_source_file",
        )
    )


# ========================= MAIN FUNCTION =========================
def main():
    spark = SparkClient()
    last_processed_date = get_last_processed_date(spark)
    print(f"Last processed date: {last_processed_date}")
    last_processed_date_dt = datetime.strptime(last_processed_date, "%Y-%m-%d")
    # Read new data based on checkpoint value
    df = read_new_data(spark, BASE_INPUT_PATH, last_processed_date_dt)

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

    print("=== Incremental load finished ===")


if __name__ == "__main__":
    main()
