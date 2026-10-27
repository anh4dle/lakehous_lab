from datetime import datetime, timezone, date
import uuid
from pyspark.sql import Window
from pyspark.sql import functions as F
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_timestamp
from typing import List, Union
from pyspark.sql.functions import (
    to_date,
)
from src.client.minio_client import MinioClient
from src.core.logger import get_logger
from src.core.config import (
    REGISTRY_TABLE,
)

logger = get_logger(__name__)


def get_current_utc_string() -> str:
    """Returns a deterministic UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_max_available_date(
    spark_client, source_name: str, source_type: str = "iceberg"
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
        max_date = MinioClient().get_max_raw_date(SOURCE_NAME=source_name)
        return max_date
    # ------------------------------------------------------------------
    # ICEBERG
    # ------------------------------------------------------------------
    if source_type == "iceberg":
        logger.info("Checking Iceberg max date: %s", source_name)
        row = (
            spark_client.spark.read.format("iceberg")
            .load(f"{source_name}.partitions")
            .selectExpr("max(partition.event_time_day) AS max_event_time")
            .first()
        )
        if row is None or row["max_event_time"] is None:
            logger.info("Iceberg table contains no data: %s", source_name)
            return None
        max_event_time = row["max_event_time"]
        # Convert to a Python date without applying to_date() in Spark.
        if isinstance(max_event_time, datetime):
            max_date = max_event_time.date()
        elif isinstance(max_event_time, date):
            max_date = max_event_time
        logger.info("Iceberg source max available date: %s", max_date)
        return max_date
    raise ValueError(
        f"Unsupported source_type: {source_type}. Expected 'raw' or 'iceberg'."
    )

def deduplicate_latest_ingested(
    df: DataFrame,
    primary_keys: list[str],
) -> DataFrame:
    window = (
        Window
        .partitionBy(*primary_keys)
        .orderBy(
            F.col("_ingested_at").desc_nulls_last()
        )
    )

    return (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
    

def _as_date(value: date | datetime) -> date:
    """Normalize a date or datetime to a date."""
    if isinstance(value, datetime):
        return value.date()
    return value


def read_new_data(
    spark_client,
    source: str,
    start_dt: datetime,
    end_dt: date | datetime,
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

    if start_dt > end_dt:
        logger.info(
            "No new data for %s. Start date=%s, end date=%s",
            pipeline_name,
            start_dt,
            end_dt,
        )
        return None

    logger.info(
        "Pipeline: %s; processing %s through %s",
        pipeline_name,
        start_dt,
        end_dt,
    )

    # ------------------------------------------------------------------
    # RAW: discover partitions through MinIO, then read through Spark/S3A
    # ------------------------------------------------------------------
    if source_type == "raw":
        client = minio_client or MinioClient()
        available_dates = client.list_raw_partition_dates(source)
        dates_to_read = [
            partition_date
            for partition_date in available_dates
            if start_dt <= partition_date <= end_dt
        ]

        if not dates_to_read:
            logger.info(
                "No Raw partitions found between %s and %s: %s",
                start_dt,
                end_dt,
                source,
            )
            return None

        base_path = source.rstrip("/")

        paths = [
            f"{base_path}/dt={partition_date.isoformat()}/"
            for partition_date in dates_to_read
        ]

        logger.info("Reading Raw partitions:")
        for path in paths:
            logger.info("  %s", path)

        df_raw = (
            spark_client.spark.read.option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .json(paths)
        )

    # ------------------------------------------------------------------
    # BRONZE / SILVER / GOLD: read an Iceberg table
    # ------------------------------------------------------------------
    elif source_type == "iceberg":
        logger.info("Reading Iceberg table: %s", source)

        df_raw = spark_client.spark.table(source).filter(
            to_date(col("event_time")).between(
                lit(start_dt),
                lit(end_dt),
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


def get_next_date(spark_client, pipeline_name) -> str:
    """Retrieve last successful processed date"""
    df = spark_client.spark.sql(
        f"""
        SELECT COALESCE(MAX(next_date), '2026-05-01') as last_date 
        FROM {REGISTRY_TABLE}
        WHERE pipeline_name = '{pipeline_name}'
    """
    )
    return df.collect()[0]["last_date"]


def update_registry_table(spark_client, max_date: str, pipeline_name):
    """Record successful load"""
    control_df = spark_client.spark.createDataFrame(
        [(pipeline_name, max_date, datetime.now(), "SUCCESS")],
        ["pipeline_name", "next_date", "load_timestamp", "status"],
    )
    control_df.writeTo(REGISTRY_TABLE).append()




def write_to_iceberg(
    df: DataFrame,
    table_name: str,
    primary_keys: Union[str, List[str]] = "event_id",
    partition_date_col: str = "event_time",
):
    """
    Executes an idempotent MERGE INTO against an Iceberg table using
    custom primary key(s) (e.g., 'event_id' for Silver, ['report_date', 'country'] for Gold).
    """
    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]

    if "gold" in table_name:
        partition_date_col = "report_date"
    # 1. Deduplicate batch on the table's specific composite key
    df_deduped = deduplicate_latest_ingested(df, primary_keys)
    # 2. Get min/max dates from the incoming batch to prune target partitions
    date_bounds = df_deduped.selectExpr(
        f"MIN(CAST({partition_date_col} AS DATE)) as min_d",
        f"MAX(CAST({partition_date_col} AS DATE)) as max_d",
    ).first()

    min_date = date_bounds["min_d"]
    max_date = date_bounds["max_d"]

    # 3. Build join predicate: "target.key1 = source.key1 AND target.key2 = source.key2"
    join_predicate = " AND ".join(
        [f"target.{key} = source.{key}" for key in primary_keys]
    )
    # 4. Add date range filter to the join predicate
    if min_date and max_date:
        join_predicate += f" AND CAST(target.{partition_date_col} AS DATE) BETWEEN '{min_date}' AND '{max_date}'"

    # 5. Register temporary view
    temp_view_name = f"staging_{uuid.uuid4().hex[:8]}"
    df_deduped.createOrReplaceTempView(temp_view_name)

    try:
        cols = df_deduped.columns
        set_clause = ", ".join(f"`{c}` = s.`{c}`" for c in cols)
        insert_cols = ", ".join(f"`{c}`" for c in cols)
        insert_vals = ", ".join(f"s.`{c}`" for c in cols)

        merge_sql = f"""
            MERGE INTO {table_name} AS t
            USING {temp_view_name} AS s
            ON {" AND ".join(f"t.`{k}` = s.`{k}`" for k in primary_keys)}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        logger.info("Executing Iceberg MERGE SQL:\n%s", merge_sql)
        logger.info("Source schema:\n%s", df_deduped.schema.simpleString())
        logger.info(
            "Target schema:\n%s",
            df.sparkSession.table(table_name).schema.simpleString(),
        )

        df.sparkSession.sql(merge_sql)
    finally:
        df.sparkSession.catalog.dropTempView(temp_view_name)



