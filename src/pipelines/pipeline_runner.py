# src/engine/base_runner.py
from datetime import datetime, timedelta
from typing import Callable, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp
from src.client.spark_client import SparkClient
from src.utils.registry import get_last_processed_date, update_registry_table
from src.utils.spark_io import read_new_data, write_to_iceberg, get_max_available_date
from src.core.logger import get_logger
from src.utils.validation import validate_and_split_dataframe

logger = get_logger(__name__)


class LakehousePipelineRunner:
    """Runner for Bronze, Silver, and Gold pipelines."""

    def __init__(self, spark_client: SparkClient):
        self.spark_client = spark_client

    def run_single_source_pipeline(
        self,
        pipeline_name: str,
        input_path: str,
        source_type: str,  # 'raw' or 'iceberg'
        target_table: str,
        primary_keys: List[str],
        transform_fn: Callable[[DataFrame], DataFrame],
    ):
        """Runs single-input pipelines (e.g., Raw -> Bronze, Bronze -> Silver)."""
        logger.info("Starting pipeline: %s", pipeline_name)
        source_max_date = get_max_available_date(
            self.spark_client, input_path, source_type
        )

        last_date_str = get_last_processed_date(self.spark_client, pipeline_name)
        last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")

        df_source = read_new_data(
            spark_client=self.spark_client,
            source=input_path,
            last_processed_date=last_dt,
            source_max_date=source_max_date,
            pipeline_name=pipeline_name,
            source_type=source_type,
        )
        df_source, df_quarantine = validate_and_split_dataframe(df_source, target_table)
        if df_quarantine and df_quarantine.count() > 0:
            logger.error(
                "Found %d malformed rows for pipeline %s. Routing to DLQ.",
                df_quarantine.count(),
                pipeline_name,
            )
            # Add an error metadata column for auditing
            quarantine_with_meta = df_quarantine.withColumn(
                "_error_reason", lit("Data Validation Failure")
            ).withColumn(
                "_quarantine_timestamp", current_timestamp()
            )
            # Write to your DLQ Iceberg table
            write_to_iceberg(
                df=quarantine_with_meta,
                table_name="iceberg.ops.quarantine_dlq",
                primary_keys=["event_id"], # or unique identifier
            )
        if df_source and df_source.count() > 0:
            df_transformed = transform_fn(df_source)
            try:
                write_to_iceberg(
                    df=df_transformed,
                    table_name=target_table,
                    primary_keys=primary_keys,
                )

                next_checkpoint = (source_max_date + timedelta(days=1)).strftime("%Y-%m-%d")
                update_registry_table(self.spark_client, next_checkpoint, pipeline_name)
                logger.info("Successfully processed pipeline %s", pipeline_name)
            except Exception:
                logger.exception("Failed to process pipeline %s", pipeline_name)
        else:
            logger.info("No new data found for %s", pipeline_name)

    def run_multi_source_pipeline(
        self,
        pipeline_name: str,
        sources: dict,
        target_table: str,
        primary_keys: List[str],
        transform_fn: Callable[..., DataFrame],
    ):
        """Runs multi-input pipelines (e.g., Silver -> Gold joins)."""
        logger.info("Starting multi-source pipeline: %s", pipeline_name)

        max_date_orders = get_max_available_date(
            self.spark_client, sources["silver_orders_df"]
        )
        max_date_customers = get_max_available_date(
            self.spark_client, sources["silver_customers_df"]
        )
        max_date = min(max_date_orders, max_date_customers)
        logger.info(
            "Max available date for %s: %s",
            sources["silver_orders_df"],
            max_date_orders,
        )
        logger.info(
            "Max available date for %s: %s",
            sources["silver_customers_df"],
            max_date_customers,
        )

        last_date_str = get_last_processed_date(self.spark_client, pipeline_name)
        last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")

        read_dfs = {}
        for alias, table_name in sources.items():
            read_dfs[alias] = read_new_data(
                spark_client=self.spark_client,
                source=table_name,
                last_processed_date=last_dt,
                source_max_date=max_date,
                pipeline_name=pipeline_name,
                source_type="iceberg",
            )

        # Ensure both sources have new data before performing join
        if all(df.count() > 0 for df in read_dfs.values()):
            df_transformed = transform_fn(**read_dfs)

            write_to_iceberg(
                df=df_transformed,
                table_name=target_table,
                primary_keys=primary_keys,
            )

            next_checkpoint = (max_date + timedelta(days=1)).strftime("%Y-%m-%d")
            update_registry_table(self.spark_client, next_checkpoint, pipeline_name)
            logger.info("Successfully processed multi-source pipeline %s", pipeline_name)
        else:
            logger.info("Skipping execution: one or more sources contain no new data.")
