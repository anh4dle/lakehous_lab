# src/engine/base_runner.py
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, current_timestamp
from src.utils.spark_io import (
    read_new_data,
    write_to_iceberg,
    get_max_available_date,
    get_next_date,
    update_registry_table,
)
from src.core.logger import get_logger
from src.utils.validation import validate_and_split_dataframe
from contextlib import contextmanager
from time import perf_counter
from src.core.config import BaseConfig, ProcessingConfig

logger = get_logger(__name__)
import logging


logger = logging.getLogger(__name__)


@contextmanager
def timed_stage(stage_name: str):
    start = perf_counter()
    try:
        yield
    finally:
        elapsed = perf_counter() - start
        logger.info("[TIMING] %-35s %.3f seconds", stage_name, elapsed)


class LakehousePipelineRunner:
    def __init__(self, spark_client):
        self.spark_client = spark_client

    def run_single_source_pipeline(
        self,
        pipeline_config: BaseConfig,
        date_plan: ProcessingConfig,
    ):
        total_start = perf_counter()
        pipeline_name = pipeline_config.PIPELINE_NAME
        SOURCE_NAME = pipeline_config.SOURCE_NAME
        target_table = pipeline_config.TABLE_NAME
        primary_keys = pipeline_config.PRIMARY_KEYS
        source_type = pipeline_config.SOURCE_TYPE
        transform_fn = pipeline_config.TRANSFORM_FN
        logger.info("Starting single-source pipeline: %s", pipeline_name)
        if date_plan is None:
            logger.info("No new data found for %s", pipeline_name)
            return
        try:
            start_dt = date_plan.start_date
            end_dt = date_plan.end_date
            with timed_stage(f"{pipeline_name} | build source read"):
                df_source = read_new_data(
                    spark_client=self.spark_client,
                    source=SOURCE_NAME,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    pipeline_name=pipeline_name,
                    source_type=source_type,
                )

            if df_source is None:
                logger.info("No new data found for %s", pipeline_name)
                return

            with timed_stage(f"{pipeline_name} | build validation plans"):
                df_source, df_quarantine = validate_and_split_dataframe(
                    df_source, target_table
                )

            # Count quarantine rows (triggers Spark action & measures true cost)
            quarantine_count = 0
            if df_quarantine is not None:
                with timed_stage(f"{pipeline_name} | count quarantine rows"):
                    quarantine_count = df_quarantine.count()

            if quarantine_count > 0:
                logger.warning(
                    "Found %d malformed rows for pipeline %s. Routing to DLQ.",
                    quarantine_count,
                    pipeline_name,
                )

                with timed_stage(f"{pipeline_name} | build quarantine metadata"):
                    quarantine_with_meta = df_quarantine.withColumn(
                        "_error_reason", lit("Data Validation Failure")
                    ).withColumn("_quarantine_timestamp", current_timestamp())

                with timed_stage(f"{pipeline_name} | write quarantine DLQ"):
                    write_to_iceberg(
                        df=quarantine_with_meta,
                        table_name="iceberg.ops.quarantine_dlq",
                        primary_keys=primary_keys,
                    )

            # Count valid rows (triggers Spark action)
            source_count = 0
            if df_source is not None:
                with timed_stage(f"{pipeline_name} | count valid rows"):
                    source_count = df_source.count()

            if source_count == 0:
                logger.info("No valid new data found for %s", pipeline_name)
                return

            logger.info(
                "Pipeline %s: valid_rows=%d, quarantine_rows=%d",
                pipeline_name,
                source_count,
                quarantine_count,
            )

            with timed_stage(f"{pipeline_name} | build transformation"):
                df_transformed = transform_fn(df_source)

            try:
                with timed_stage(f"{pipeline_name} | Iceberg write/merge"):
                    write_to_iceberg(
                        df=df_transformed,
                        table_name=target_table,
                        primary_keys=primary_keys,
                    )

                with timed_stage(f"{pipeline_name} | update registry"):
                    next_checkpoint = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

                    update_registry_table(
                        self.spark_client,
                        next_checkpoint,
                        pipeline_name,
                    )

                logger.info("Successfully processed pipeline %s", pipeline_name)

            except Exception:
                logger.exception("Failed to process pipeline %s", pipeline_name)
                raise

        finally:
            logger.info(
                "[TIMING] %-35s %.3f seconds",
                f"{pipeline_name} | TOTAL",
                perf_counter() - total_start,
            )

    def run_multi_source_pipeline(
        self,
        pipeline_config: BaseConfig,
        sources: dict,
        plan: ProcessingConfig,
    ):
        total_start = perf_counter()
        pipeline_name = pipeline_config.PIPELINE_NAME
        target_table = pipeline_config.TABLE_NAME
        primary_keys = pipeline_config.PRIMARY_KEYS
        transform_fn = pipeline_config.TRANSFORM_FN
        start_date = plan.start_date
        end_date = plan.end_date
        logger.info("Starting multi-source pipeline: %s", pipeline_name)

        try:
            read_dfs = {}
            for alias, table_name in sources.items():
                with timed_stage(f"{pipeline_name} | build read: {alias}"):
                    read_dfs[alias] = read_new_data(
                        spark_client=self.spark_client,
                        source=table_name,
                        start_dt=start_date,
                        end_dt=end_date,
                        pipeline_name=pipeline_name,
                        source_type="iceberg",
                    )

            if any(df is None for df in read_dfs.values()):
                logger.info(
                    "Skipping %s: at least one source has no new data.",
                    pipeline_name,
                )
                return

            # Measure each source count separately (triggers Spark action)
            source_counts = {}
            for alias, df in read_dfs.items():
                with timed_stage(f"{pipeline_name} | count source: {alias}"):
                    source_counts[alias] = df.count()

            logger.info(
                "Pipeline %s source row counts: %s",
                pipeline_name,
                source_counts,
            )

            if any(count == 0 for count in source_counts.values()):
                logger.info(
                    "Skipping %s: one or more sources contain no new data.",
                    pipeline_name,
                )
                return

            with timed_stage(f"{pipeline_name} | build transformation"):
                df_transformed = transform_fn(**read_dfs)

            with timed_stage(f"{pipeline_name} | Iceberg write/merge"):
                write_to_iceberg(
                    df=df_transformed,
                    table_name=target_table,
                    primary_keys=primary_keys,
                )

            with timed_stage(f"{pipeline_name} | update registry"):
                next_checkpoint = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

                update_registry_table(
                    self.spark_client,
                    next_checkpoint,
                    pipeline_name,
                )

            logger.info(
                "Successfully processed multi-source pipeline %s",
                pipeline_name,
            )

        finally:
            logger.info(
                "[TIMING] %-35s %.3f seconds",
                f"{pipeline_name} | TOTAL",
                perf_counter() - total_start,
            )
