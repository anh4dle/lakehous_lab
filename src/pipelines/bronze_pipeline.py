from src.config.config import CustomerBronzeConfig, OrderBronzeConfig
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.transform.transform_script import (
    transform_to_bronze_customer_events,
    transform_to_bronze_orders_events,
)
from src.client.spark_client import SparkClient
from src.client.minio_client import MinioClient


def run_bronze_pipeline():
    # Process Customer Bronze
    spark_client = SparkClient()
    runner = LakehousePipelineRunner(spark_client)
    runner.run_single_source_pipeline(
        pipeline_name=CustomerBronzeConfig.PIPELINE_NAME,
        input_path=CustomerBronzeConfig.INPUT_PATH,
        source_type=CustomerBronzeConfig.SOURCE_TYPE,
        target_table=CustomerBronzeConfig.TABLE_NAME,
        primary_keys=["event_id"],
        transform_fn=transform_to_bronze_customer_events,
    )

    # Process Order Bronze
    runner.run_single_source_pipeline(
        pipeline_name=OrderBronzeConfig.PIPELINE_NAME,
        input_path=OrderBronzeConfig.INPUT_PATH,
        source_type=OrderBronzeConfig.SOURCE_TYPE,
        target_table=OrderBronzeConfig.TABLE_NAME,
        primary_keys=["event_id"],
        transform_fn=transform_to_bronze_orders_events,
    )


if __name__ == "__main__":
    run_bronze_pipeline()
