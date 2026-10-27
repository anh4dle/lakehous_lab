# src/engine/pipeline_runner.py
from datetime import timedelta
from pyspark.sql.functions import lit, current_timestamp
from src.utils.spark_io import (
    read_new_data,
    write_to_iceberg,
    update_registry_table,
)
from src.core.logger import get_logger
from src.utils.validation import validate_and_split_dataframe
from src.core.config import BaseConfig, ProcessingConfig
logger = get_logger(__name__)


class LakehousePipelineRunner:
    def __init__(self, spark_client):
        self.spark_client = spark_client

    def run_single_source_pipeline(
        self,
        pipeline_config: BaseConfig,
        date_plan: ProcessingConfig,
    ):
        #Get configs
        pipeline_name = pipeline_config.PIPELINE_NAME
        source_name = pipeline_config.SOURCE_NAME
        target_table = pipeline_config.TABLE_NAME
        primary_keys = pipeline_config.PRIMARY_KEYS
        source_type = pipeline_config.SOURCE_TYPE
        transform_fn = pipeline_config.TRANSFORM_FN
        logger.info("Starting single-source pipeline: %s", pipeline_name)
        if date_plan is None:
            logger.info("No new data found for %s", pipeline_name)
            return
        start_dt = date_plan.start_date
        end_dt = date_plan.end_date
        
        #Read new data from source
        df_source = read_new_data(
            spark_client=self.spark_client,
            source=source_name,
            start_dt=start_dt,
            end_dt=end_dt,
            pipeline_name=pipeline_name,
            source_type=source_type,
        )

        if df_source is None:
            logger.info("No new data found for %s", pipeline_name)
            return
        if source_type == "iceberg":
            #Validate source schemas, business rules, and split into clean and quarantine
            df_source, df_quarantine = validate_and_split_dataframe(
                df_source, source_name
            )
            self.write_quarantine_rows(df_quarantine, pipeline_name, primary_keys)
        #Transform source data to target table schema
        df_transformed = transform_fn(df_source)
        self.merge_and_update_registry_table(df_transformed, date_plan, pipeline_name, target_table, primary_keys)

    def run_multi_source_pipeline(
        self,
        pipeline_config: BaseConfig,
        sources: dict,
        plan: ProcessingConfig,
    ):
        #Get pipeline config
        pipeline_name = pipeline_config.PIPELINE_NAME
        target_table = pipeline_config.TABLE_NAME
        primary_keys = pipeline_config.PRIMARY_KEYS
        transform_fn = pipeline_config.TRANSFORM_FN

        logger.info("Starting multi-source pipeline: %s", pipeline_name)
        if plan is None:
            logger.info("No new data found for %s", pipeline_name)
            return
        
        start_date = plan.start_date
        end_date = plan.end_date
        read_dfs = {}
        quarantine_dfs = {}
        #Read source table into DataFrames
        for alias, table_name in sources.items():
            df_source = read_new_data(
                spark_client=self.spark_client,
                source=table_name,
                start_dt=start_date,
                end_dt=end_date,
                pipeline_name=pipeline_name,
                source_type="iceberg",
            )
            df_source, df_quarantine = validate_and_split_dataframe(
                df_source, table_name
            )
            read_dfs[alias] = df_source
            quarantine_dfs[alias] = df_quarantine
            

        if any(df is None for df in read_dfs.values()):
            logger.info(
                "Skipping %s: at least one source has no new data.",
                pipeline_name,
            )
            return
        #Transform source data to target table schema
        df_transformed = transform_fn(**read_dfs)
        for k, df_quarantine in quarantine_dfs.items():
            self.write_quarantine_rows(df_quarantine, pipeline_name, primary_keys)
        self.merge_and_update_registry_table(df_transformed, plan, pipeline_name, target_table, primary_keys)
        
    def merge_and_update_registry_table(
        self,
        df_transformed,
        plan: ProcessingConfig,
        pipeline_name: str,
        target_table: str,
        primary_keys: list[str]
    ):
        try:
            df_count = df_transformed.count()
            write_to_iceberg(
                df=df_transformed,
                table_name=target_table,
                primary_keys=primary_keys,
            )

            if plan.mode == "backfill":
                return
            
            next_checkpoint = (plan.end_date + timedelta(days=1)).strftime("%Y-%m-%d")

            update_registry_table(
                self.spark_client,
                next_checkpoint,
                pipeline_name,
            )

            logger.info(
                "Successfully processed %d rows to %s",
                df_count,
                pipeline_name,
            )
        except Exception:
            logger.exception("Failed to process pipeline %s", pipeline_name)
            raise
    
    def write_quarantine_rows(self, df_quarantine, pipeline_name, primary_keys):
        quarantine_count = 0
        
        if df_quarantine is not None:
            quarantine_count = df_quarantine.count()

        #Write malformed rows to quarantine table
        if quarantine_count > 0:
            logger.warning(
                "Found %d malformed rows for pipeline %s. Routing to DLQ.",
                quarantine_count,
                pipeline_name,
            )

            quarantine_with_meta = df_quarantine.withColumn(
                "_error_reason", lit("Data Validation Failure")
            ).withColumn("_quarantine_timestamp", current_timestamp())

            write_to_iceberg(
                df=quarantine_with_meta,
                table_name="iceberg.ops.quarantine_dlq",
                primary_keys=primary_keys,
            )
            logger.info("Successfully inserted %d rows to quarantine table", quarantine_count)