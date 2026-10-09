from datetime import datetime, timedelta, timezone, date
import uuid
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_timestamp
from typing import List, Union
from pyspark.sql.functions import (
    max as spark_max,
    to_date,
)
from src.client.minio_client import MinioClient


def get_current_utc_string() -> str:
    """Returns a deterministic UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_max_available_date(
    spark_client, input_path: str, source_type: str = "iceberg"
) -> datetime | None:
    """
    Return the latest date currently available in the source.
    Raw:
        MAX(dt=YYYY-MM-DD partition)
    Iceberg:
        MAX(DATE(event_time))
    """

    # ------------------------------------------------------------------
    # RAW / MINIO, get the max partition date from the S3 bucket
    # ------------------------------------------------------------------
    if source_type == "raw":
        max_date = MinioClient().get_max_raw_date(input_path=input_path)
        return max_date
    # ------------------------------------------------------------------
    # ICEBERG
    # ------------------------------------------------------------------
    if source_type == "iceberg":
        print(f"Checking Iceberg max date: {input_path}")
        row = (
            spark_client.spark.table(input_path)
            .select(spark_max(to_date(col("event_time"))).alias("max_date"))
            .first()
        )
        if row is None or row["max_date"] is None:
            print(f"Iceberg table contains no data: {input_path}")
            return None

        max_date = row["max_date"]
        print(f"Iceberg source max available date: {max_date}")
        return max_date
    raise ValueError(
        f"Unsupported source_type: {source_type}. Expected 'raw' or 'iceberg'."
    )


# def read_new_data(
#     spark_client,
#     source: str,
#     last_processed_date: datetime,
#     source_max_date: datetime,
#     pipeline_name: str,
#     source_type="iceberg",
# ) -> DataFrame:
#     """Read the next unprocessed day's data from Raw or Bronze."""
#     start_date = (last_processed_date + timedelta(days=1)).strftime("%Y-%m-%d")
#     end_date = (source_max_date).strftime("%Y-%m-%d")
#     # ------------------------------------------------------------------
#     # RAW: JSON files in S3
#     # ------------------------------------------------------------------

#     if source_type == "raw":
#         path = f"{source}/dt={next_date}/"
#         print(f"Reading new data from {path}")
#         print("Input format: json")

#         df_raw = (
#             spark_client.spark.read.option("multiLine", "true")
#             .option("mode", "PERMISSIVE")
#             .json(path)
#         )

#     # ------------------------------------------------------------------
#     # BRONZE/SILVER/GOLD: Iceberg table
#     # ------------------------------------------------------------------

#     elif source_type == "iceberg":
#         print(f"Reading new data from Iceberg table: {source}")
#         print(f"Processing date: {next_date}")
#         print("Input format: iceberg")

#         df_raw = spark_client.spark.table(source).filter(
#             col("event_time").cast("date") == lit(next_date).cast("date")
#         )

#     else:
#         raise ValueError(
#             f"Unsupported source_type: {source_type}. Expected 'raw' or 'iceberg'."
#         )

#     # ------------------------------------------------------------------
#     # Add pipeline metadata
#     # ------------------------------------------------------------------

#     batch_time = get_current_utc_string()

#     return (
#         df_raw.withColumn(
#             "pipeline_name",
#             lit(pipeline_name),
#         )
#         .withColumn(
#             "event_time",
#             to_timestamp(
#                 col("event_time"),
#                 "yyyy-MM-dd'T'HH:mm:ss",
#             ),
#         )
#         .withColumn(
#             "_ingested_at",
#             to_timestamp(lit(batch_time)),
#         )
#     )


def write_to_iceberg(
    df: DataFrame, table_name: str, primary_keys: Union[str, List[str]] = "event_id"
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


def _as_date(value: date | datetime) -> date:
    """Normalize a date or datetime to a date."""
    if isinstance(value, datetime):
        return value.date()
    return value


def read_new_data(
    spark_client,
    source: str,
    last_processed_date: datetime,
    source_max_date: date | datetime,
    pipeline_name: str,
    source_type: str = "iceberg",
    minio_client: Optional[MinioClient] = None,
) -> Optional[DataFrame]:
    """
    Read data from the day after the checkpoint through source_max_date,
    inclusive.

    Raw:
        Discover existing MinIO partitions and read their S3A paths.

    Iceberg:
        Filter rows by event_time date.
    """
    start_date = _as_date(last_processed_date) + timedelta(days=1)
    end_date = _as_date(source_max_date)

    if start_date > end_date:
        print(
            f"No new data for {pipeline_name}. "
            f"Start date={start_date}, end date={end_date}"
        )
        return None

    print(f"Pipeline: {pipeline_name}; processing {start_date} through {end_date}")

    # ------------------------------------------------------------------
    # RAW: discover partitions through MinIO, then read through Spark/S3A
    # ------------------------------------------------------------------
    if source_type == "raw":
        client = minio_client or MinioClient()

        available_dates = client.list_raw_partition_dates(source)

        dates_to_read = [
            partition_date
            for partition_date in available_dates
            if start_date <= partition_date <= end_date
        ]

        if not dates_to_read:
            print(
                f"No Raw partitions found between {start_date} and {end_date}: {source}"
            )
            return None

        base_path = source.rstrip("/")

        paths = [
            f"{base_path}/dt={partition_date.isoformat()}/"
            for partition_date in dates_to_read
        ]

        print("Reading Raw partitions:")
        for path in paths:
            print(f"  {path}")

        df_raw = (
            spark_client.spark.read.option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .json(paths)
        )

    # ------------------------------------------------------------------
    # BRONZE / SILVER / GOLD: read an Iceberg table
    # ------------------------------------------------------------------
    elif source_type == "iceberg":
        print(f"Reading Iceberg table: {source}")

        df_raw = spark_client.spark.table(source).filter(
            to_date(col("event_time")).between(
                lit(start_date),
                lit(end_date),
            )
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
        df_raw.withColumn("pipeline_name", lit(pipeline_name))
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
