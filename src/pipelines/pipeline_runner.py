# # src/engine/base_runner.py
# from datetime import datetime, timedelta
# from typing import Callable, List
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import lit, current_timestamp
# from src.client.spark_client import SparkClient
# from src.utils.registry import get_next_date, update_registry_table
# from src.utils.spark_io import read_new_data, write_to_iceberg, get_max_available_date
# from src.core.logger import get_logger
# from src.utils.validation import validate_and_split_dataframe
# from contextlib import contextmanager
# from time import perf_counter

# logger = get_logger(__name__)

# @contextmanager
# def timed_stage(stage_name: str):
#     start = perf_counter()
#     try:
#         yield
#     finally:
#         elapsed = perf_counter() - start
#         logger.info("[TIMING] %-35s %.3f seconds", stage_name, elapsed)
        

# class LakehousePipelineRunner:
#     """Runner for Bronze, Silver, and Gold pipelines."""

#     def __init__(self, spark_client: SparkClient):
#         self.spark_client = spark_client

#     # def run_single_source_pipeline(
#     #     self,
#     #     pipeline_name: str,
#     #     input_path: str,
#     #     source_type: str,  # 'raw' or 'iceberg'
#     #     target_table: str,
#     #     primary_keys: List[str],
#     #     transform_fn: Callable[[DataFrame], DataFrame],
#     # ):
#     #     """Runs single-input pipelines (e.g., Raw -> Bronze, Bronze -> Silver)."""
#     #     logger.info("Starting pipeline: %s", pipeline_name)
#     #     source_max_date = get_max_available_date(
#     #         self.spark_client, input_path, source_type
#     #     )

#     #     last_date_str = get_next_date(self.spark_client, pipeline_name)
#     #     last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")

#     #     df_source = read_new_data(
#     #         spark_client=self.spark_client,
#     #         source=input_path,
#     #         next_date=last_dt,
#     #         source_max_date=source_max_date,
#     #         pipeline_name=pipeline_name,
#     #         source_type=source_type,
#     #     )
#     #     df_source, df_quarantine = validate_and_split_dataframe(df_source, target_table)
#     #     if df_quarantine and df_quarantine.count() > 0:
#     #         logger.error(
#     #             "Found %d malformed rows for pipeline %s. Routing to DLQ.",
#     #             df_quarantine.count(),
#     #             pipeline_name,
#     #         )
#     #         # Add an error metadata column for auditing
#     #         quarantine_with_meta = df_quarantine.withColumn(
#     #             "_error_reason", lit("Data Validation Failure")
#     #         ).withColumn(
#     #             "_quarantine_timestamp", current_timestamp()
#     #         )
#     #         # Write to your DLQ Iceberg table
#     #         write_to_iceberg(
#     #             df=quarantine_with_meta,
#     #             table_name="iceberg.ops.quarantine_dlq",
#     #             primary_keys=["event_id"], # or unique identifier
#     #         )
#     #     if df_source and df_source.count() > 0:
#     #         df_transformed = transform_fn(df_source)
#     #         try:
#     #             write_to_iceberg(
#     #                 df=df_transformed,
#     #                 table_name=target_table,
#     #                 primary_keys=primary_keys,
#     #             )

#     #             next_checkpoint = (source_max_date + timedelta(days=1)).strftime("%Y-%m-%d")
#     #             update_registry_table(self.spark_client, next_checkpoint, pipeline_name)
#     #             logger.info("Successfully processed pipeline %s", pipeline_name)
#     #         except Exception:
#     #             logger.exception("Failed to process pipeline %s", pipeline_name)
#     #     else:
#     #         logger.info("No new data found for %s", pipeline_name)

#     # def run_multi_source_pipeline(
#     #     self,
#     #     pipeline_name: str,
#     #     sources: dict,
#     #     target_table: str,
#     #     primary_keys: List[str],
#     #     transform_fn: Callable[..., DataFrame],
#     # ):
#     #     """Runs multi-input pipelines (e.g., Silver -> Gold joins)."""
#     #     logger.info("Starting multi-source pipeline: %s", pipeline_name)

#     #     max_date_orders = get_max_available_date(
#     #         self.spark_client, sources["silver_orders_df"]
#     #     )
#     #     max_date_customers = get_max_available_date(
#     #         self.spark_client, sources["silver_customers_df"]
#     #     )
#     #     max_date = min(max_date_orders, max_date_customers)
#     #     logger.info(
#     #         "Max available date for %s: %s",
#     #         sources["silver_orders_df"],
#     #         max_date_orders,
#     #     )
#     #     logger.info(
#     #         "Max available date for %s: %s",
#     #         sources["silver_customers_df"],
#     #         max_date_customers,
#     #     )

#     #     last_date_str = get_next_date(self.spark_client, pipeline_name)
#     #     last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")

#     #     read_dfs = {}
#     #     for alias, table_name in sources.items():
#     #         read_dfs[alias] = read_new_data(
#     #             spark_client=self.spark_client,
#     #             source=table_name,
#     #             next_date=last_dt,
#     #             source_max_date=max_date,
#     #             pipeline_name=pipeline_name,
#     #             source_type="iceberg",
#     #         )

#     #     # Ensure both sources have new data before performing join
#     #     if all(df.count() > 0 for df in read_dfs.values()):
#     #         df_transformed = transform_fn(**read_dfs)

#     #         write_to_iceberg(
#     #             df=df_transformed,
#     #             table_name=target_table,
#     #             primary_keys=primary_keys,
#     #         )

#     #         next_checkpoint = (max_date + timedelta(days=1)).strftime("%Y-%m-%d")
#     #         update_registry_table(self.spark_client, next_checkpoint, pipeline_name)
#     #         logger.info("Successfully processed multi-source pipeline %s", pipeline_name)
#     #     else:
#     #         logger.info("Skipping execution: one or more sources contain no new data.")


#     def run_single_source_pipeline(
#         self,
#         pipeline_name: str,
#         input_path: str,
#         source_type: str,
#         target_table: str,
#         primary_keys: List[str],
#         transform_fn: Callable[[DataFrame], DataFrame],
#     ):
#         total_start = perf_counter()
#         logger.info("Starting pipeline: %s", pipeline_name)

#         try:
#             with timed_stage(f"{pipeline_name} | get max source date"):
#                 source_max_date = get_max_available_date(
#                     self.spark_client, input_path, source_type
#                 )

#             with timed_stage(f"{pipeline_name} | get checkpoint"):
#                 last_date_str = get_next_date(
#                     self.spark_client, pipeline_name
#                 )
#                 last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")

#             with timed_stage(f"{pipeline_name} | build source read"):
#                 df_source = read_new_data(
#                     spark_client=self.spark_client,
#                     source=input_path,
#                     next_date=last_dt,
#                     source_max_date=source_max_date,
#                     pipeline_name=pipeline_name,
#                     source_type=source_type,
#                 )

#             if df_source is None:
#                 logger.info("No new data found for %s", pipeline_name)
#                 return

#             with timed_stage(f"{pipeline_name} | build validation plans"):
#                 df_source, df_quarantine = validate_and_split_dataframe(
#                     df_source, target_table
#                 )

#             # Count quarantine rows once.
#             quarantine_count = 0
#             if df_quarantine is not None:
#                 with timed_stage(f"{pipeline_name} | count quarantine rows"):
#                     quarantine_count = df_quarantine.count()

#             if quarantine_count > 0:
#                 logger.warning(
#                     "Found %d malformed rows for pipeline %s. Routing to DLQ.",
#                     quarantine_count,
#                     pipeline_name,
#                 )

#                 with timed_stage(f"{pipeline_name} | build quarantine metadata"):
#                     quarantine_with_meta = (
#                         df_quarantine
#                         .withColumn(
#                             "_error_reason", lit("Data Validation Failure")
#                         )
#                         .withColumn(
#                             "_quarantine_timestamp", current_timestamp()
#                         )
#                     )

#                 with timed_stage(f"{pipeline_name} | write quarantine DLQ"):
#                     write_to_iceberg(
#                         df=quarantine_with_meta,
#                         table_name="iceberg.ops.quarantine_dlq",
#                         primary_keys=["event_id"],
#                     )

#             # Count valid rows once.
#             source_count = 0
#             if df_source is not None:
#                 with timed_stage(f"{pipeline_name} | count valid rows"):
#                     source_count = df_source.count()

#             if source_count == 0:
#                 logger.info("No valid new data found for %s", pipeline_name)
#                 return

#             logger.info(
#                 "Pipeline %s: valid_rows=%d, quarantine_rows=%d",
#                 pipeline_name,
#                 source_count,
#                 quarantine_count,
#             )

#             with timed_stage(f"{pipeline_name} | build transformation"):
#                 df_transformed = transform_fn(df_source)

#             try:
#                 with timed_stage(f"{pipeline_name} | Iceberg write/merge"):
#                     write_to_iceberg(
#                         df=df_transformed,
#                         table_name=target_table,
#                         primary_keys=primary_keys,
#                     )

#                 with timed_stage(f"{pipeline_name} | update registry"):
#                     next_checkpoint = (
#                         source_max_date + timedelta(days=1)
#                     ).strftime("%Y-%m-%d")

#                     update_registry_table(
#                         self.spark_client,
#                         next_checkpoint,
#                         pipeline_name,
#                     )

#                 logger.info(
#                     "Successfully processed pipeline %s", pipeline_name
#                 )

#             except Exception:
#                 logger.exception(
#                     "Failed to process pipeline %s", pipeline_name
#                 )
#                 raise

#         finally:
#             logger.info(
#                 "[TIMING] %-35s %.3f seconds",
#                 f"{pipeline_name} | TOTAL",
#                 perf_counter() - total_start,
#             )


#     def run_multi_source_pipeline(
#         self,
#         pipeline_name: str,
#         sources: dict,
#         target_table: str,
#         primary_keys: List[str],
#         transform_fn: Callable[..., DataFrame],
#     ):
#         total_start = perf_counter()
#         logger.info("Starting multi-source pipeline: %s", pipeline_name)

#         try:
#             with timed_stage(f"{pipeline_name} | get source max dates"):
#                 max_dates = {
#                     alias: get_max_available_date(
#                         self.spark_client, table_name
#                     )
#                     for alias, table_name in sources.items()
#                 }

#                 max_date = min(max_dates.values())

#             for alias, max_date_value in max_dates.items():
#                 logger.info(
#                     "Max available date for source %s: %s",
#                     alias,
#                     max_date_value,
#                 )

#             with timed_stage(f"{pipeline_name} | get checkpoint"):
#                 last_date_str = get_next_date(
#                     self.spark_client, pipeline_name
#                 )
#                 last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")

#             read_dfs = {}
#             for alias, table_name in sources.items():
#                 with timed_stage(f"{pipeline_name} | build read: {alias}"):
#                     read_dfs[alias] = read_new_data(
#                         spark_client=self.spark_client,
#                         source=table_name,
#                         next_date=last_dt,
#                         source_max_date=max_date,
#                         pipeline_name=pipeline_name,
#                         source_type="iceberg",
#                     )

#             if any(df is None for df in read_dfs.values()):
#                 logger.info(
#                     "Skipping %s: at least one source has no new data.",
#                     pipeline_name,
#                 )
#                 return

#             # Measure each source count separately to identify slow inputs.
#             source_counts = {}
#             for alias, df in read_dfs.items():
#                 with timed_stage(f"{pipeline_name} | count source: {alias}"):
#                     source_counts[alias] = df.count()

#             logger.info(
#                 "Pipeline %s source row counts: %s",
#                 pipeline_name,
#                 source_counts,
#             )

#             if any(count == 0 for count in source_counts.values()):
#                 logger.info(
#                     "Skipping %s: one or more sources contain no new data.",
#                     pipeline_name,
#                 )
#                 return

#             with timed_stage(f"{pipeline_name} | build transformation"):
#                 df_transformed = transform_fn(**read_dfs)

#             with timed_stage(f"{pipeline_name} | Iceberg write/merge"):
#                 write_to_iceberg(
#                     df=df_transformed,
#                     table_name=target_table,
#                     primary_keys=primary_keys,
#                 )

#             with timed_stage(f"{pipeline_name} | update registry"):
#                 next_checkpoint = (
#                     max_date + timedelta(days=1)
#                 ).strftime("%Y-%m-%d")

#                 update_registry_table(
#                     self.spark_client,
#                     next_checkpoint,
#                     pipeline_name,
#                 )

#             logger.info(
#                 "Successfully processed multi-source pipeline %s",
#                 pipeline_name,
#             )

#         finally:
#             logger.info(
#                 "[TIMING] %-35s %.3f seconds",
#                 f"{pipeline_name} | TOTAL",
#                 perf_counter() - total_start,
#             )


# src/engine/base_runner.py
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, current_timestamp
from src.utils.registry import get_next_date, update_registry_table
from src.utils.spark_io import read_new_data, write_to_iceberg, get_max_available_date
from src.core.logger import get_logger
from src.utils.validation import validate_and_split_dataframe
from contextlib import contextmanager
from time import perf_counter
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
        pipeline_name: str,
        input_path: str,
        target_table: str,
        primary_keys: list[str],
        source_type: str,
        transform_fn: callable,
    ):
        total_start = perf_counter()
        logger.info("Starting single-source pipeline: %s", pipeline_name)

        try:
            with timed_stage(f"{pipeline_name} | get max source date"):
                source_max_date = get_max_available_date(
                    self.spark_client, input_path, source_type
                )

            if source_max_date is None:
                logger.info("Source table contains no data: %s", input_path)
                return

            with timed_stage(f"{pipeline_name} | get checkpoint"):
                last_date_str = get_next_date(self.spark_client, pipeline_name)
                last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")

            with timed_stage(f"{pipeline_name} | build source read"):
                df_source = read_new_data(
                    spark_client=self.spark_client,
                    source=input_path,
                    next_date=last_dt,
                    source_max_date=source_max_date,
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
                    quarantine_with_meta = (
                        df_quarantine
                        .withColumn("_error_reason", lit("Data Validation Failure"))
                        .withColumn("_quarantine_timestamp", current_timestamp())
                    )

                with timed_stage(f"{pipeline_name} | write quarantine DLQ"):
                    write_to_iceberg(
                        df=quarantine_with_meta,
                        table_name="iceberg.ops.quarantine_dlq",
                        primary_keys=["event_id"],
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
                # Optionally cache or force evaluation if transform_fn has heavy logic
                # df_transformed.cache()

            try:
                with timed_stage(f"{pipeline_name} | Iceberg write/merge"):
                    write_to_iceberg(
                        df=df_transformed,
                        table_name=target_table,
                        primary_keys=primary_keys,
                    )

                with timed_stage(f"{pipeline_name} | update registry"):
                    next_checkpoint = (
                        source_max_date + timedelta(days=1)
                    ).strftime("%Y-%m-%d")

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
        pipeline_name: str,
        sources: dict,
        target_table: str,
        primary_keys: list[str],
        transform_fn: callable,
    ):
        total_start = perf_counter()
        logger.info("Starting multi-source pipeline: %s", pipeline_name)

        try:
            with timed_stage(f"{pipeline_name} | get source max dates"):
                max_dates = {
                    alias: get_max_available_date(self.spark_client, table_name)
                    for alias, table_name in sources.items()
                }

                if not max_dates or any(d is None for d in max_dates.values()):
                    logger.info("Skipping %s: one or more sources have no max date.", pipeline_name)
                    return

                max_date = min(max_dates.values())

            for alias, max_date_value in max_dates.items():
                logger.info(
                    "Max available date for source %s: %s",
                    alias,
                    max_date_value,
                )

            with timed_stage(f"{pipeline_name} | get checkpoint"):
                last_date_str = get_next_date(self.spark_client, pipeline_name)
                last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")

            read_dfs = {}
            for alias, table_name in sources.items():
                with timed_stage(f"{pipeline_name} | build read: {alias}"):
                    read_dfs[alias] = read_new_data(
                        spark_client=self.spark_client,
                        source=table_name,
                        next_date=last_dt,
                        source_max_date=max_date,
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
                next_checkpoint = (
                    max_date + timedelta(days=1)
                ).strftime("%Y-%m-%d")

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