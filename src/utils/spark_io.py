from datetime import datetime, timedelta, timezone
import uuid
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_timestamp
from typing import List, Union
def get_current_utc_string() -> str:
    """Returns a deterministic UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def read_new_data(
    spark_client,
    source: str,
    last_processed_date: datetime,
    pipeline_name: str,
    source_type="iceberg",
) -> DataFrame:
    """Read the next unprocessed day's data from Raw or Bronze."""

    next_date = (last_processed_date + timedelta(days=1)).strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # RAW: JSON files in S3
    # ------------------------------------------------------------------

    if source_type == "raw":
        path = f"{source}/dt={next_date}/"

        print(f"Reading new data from {path}")
        print("Input format: json")

        df_raw = (
            spark_client.spark.read.option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .json(path)
        )

    # ------------------------------------------------------------------
    # BRONZE/SILVER/GOLD: Iceberg table
    # ------------------------------------------------------------------

    elif source_type == "iceberg":
        print(f"Reading new data from Iceberg table: {source}")
        print(f"Processing date: {next_date}")
        print("Input format: iceberg")

        df_raw = spark_client.spark.table(source).filter(
            col("event_time").cast("date") == lit(next_date).cast("date")
        )

    else:
        raise ValueError(
            f"Unsupported source_type: {source_type}. Expected 'raw' or 'iceberg'."
        )

    # ------------------------------------------------------------------
    # Add pipeline metadata
    # ------------------------------------------------------------------

    batch_time = get_current_utc_string()

    return (
        df_raw.withColumn(
            "pipeline_name",
            lit(pipeline_name),
        )
        .withColumn(
            "event_time",
            to_timestamp(
                col("event_time"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn(
            "_ingested_at",
            to_timestamp(lit(batch_time)),
        )
    )


def write_to_iceberg(
    df: DataFrame, 
    table_name: str, 
    primary_keys: Union[str, List[str]] = "event_id"
):
    """
    Executes an idempotent MERGE INTO against an Iceberg table using 
    custom primary key(s) (e.g., 'event_id' for Silver, ['report_date', 'country'] for Gold).
    """
    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]

    # 1. Deduplicate batch on the table's specific composite key
    df_deduped = df.dropDuplicates(primary_keys)

    # 2. Build dynamic join predicate: "target.key1 = source.key1 AND target.key2 = source.key2"
    join_predicate = " AND ".join(
        [f"target.{key} = source.{key}" for key in primary_keys]
    )

    # 3. Register temporary view
    temp_view_name = f"staging_{uuid.uuid4().hex[:8]}"
    df_deduped.createOrReplaceTempView(temp_view_name)

    try:
        df.sparkSession.sql(f"""
            MERGE INTO {table_name} AS target
            USING {temp_view_name} AS source
            ON {join_predicate}
            WHEN MATCHED THEN
              UPDATE SET *
            WHEN NOT MATCHED THEN
              INSERT *
        """)
    finally:
        df.sparkSession.catalog.dropTempView(temp_view_name)