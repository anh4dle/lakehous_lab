# src/engine/base_runner.py
from datetime import datetime, timedelta
from typing import Callable, List
from pyspark.sql import DataFrame
from src.client.minio_client import MinioClient
from src.client.spark_client import SparkClient
from src.utils.registry import get_last_processed_date, update_registry_table
from src.utils.spark_io import read_new_data, write_to_iceberg, get_max_available_date


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
        print(f"=== Starting Pipeline: {pipeline_name} ===")
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

        if df_source and df_source.count() > 0:
            df_transformed = transform_fn(df_source)

            write_to_iceberg(
                df=df_transformed,
                table_name=target_table,
                primary_keys=primary_keys,
            )

            next_checkpoint = (source_max_date + timedelta(days=1)).strftime("%Y-%m-%d")
            update_registry_table(self.spark_client, next_checkpoint, pipeline_name)
            print(f"Success: Processed pipeline {pipeline_name}")
        else:
            print(f"No new data found for {pipeline_name}")

    def run_multi_source_pipeline(
        self,
        pipeline_name: str,
        sources: dict,
        target_table: str,
        primary_keys: List[str],
        transform_fn: Callable[..., DataFrame],
    ):
        """Runs multi-input pipelines (e.g., Silver -> Gold joins)."""
        print(f"=== Starting Multi-Source Pipeline: {pipeline_name} ===")

        max_date_orders = get_max_available_date(
            self.spark_client, sources["silver_orders_df"]
        )
        max_date_customers = get_max_available_date(
            self.spark_client, sources["silver_customers_df"]
        )
        max_date = min(max_date_orders, max_date_customers)
        print(
            f"Max available date for {sources['silver_orders_df']}: {max_date_orders}"
        )
        print(
            f"Max available date for {sources['silver_customers_df']}: {max_date_customers}"
        )
        return

        last_date_str = get_last_processed_date(self.spark_client, pipeline_name)
        last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")

        read_dfs = {}
        for alias, table_name in sources.items():
            read_dfs[alias] = read_new_data(
                spark_client=self.spark_client,
                source=table_name,
                last_processed_date=last_dt,
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

            next_checkpoint = (last_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            update_registry_table(self.spark_client, next_checkpoint, pipeline_name)
            print(f"Success: Processed multi-source pipeline {pipeline_name}")
        else:
            print("Skipping execution: One or more sources contain no new data.")
