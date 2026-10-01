from src.client.spark_client import SparkClient
from src.config.config import CustomerSilverConfig, OrderSilverConfig, CustomerBronzeConfig, OrderBronzeConfig
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.transform.transform_script import (
    transform_to_silver_dim_customer,
    transform_to_silver_fact_orders,
)

if __name__ == "__main__":
    spark = SparkClient()
    runner = LakehousePipelineRunner(spark)

    # Process Customer Bronze
    runner.run_single_source_pipeline(
        pipeline_name=CustomerSilverConfig.PIPELINE_NAME,
        source_name=CustomerBronzeConfig.TABLE_NAME,
        source_type=CustomerSilverConfig.SOURCE_TYPE,
        target_table=CustomerSilverConfig.TABLE_NAME,
        primary_keys=["event_id"],
        transform_fn=transform_to_silver_dim_customer,
    )

    # Process Order Bronze
    runner.run_single_source_pipeline(
        pipeline_name=OrderSilverConfig.PIPELINE_NAME,
        source_name=OrderBronzeConfig.TABLE_NAME,
        source_type=OrderSilverConfig.SOURCE_TYPE,
        target_table=OrderSilverConfig.TABLE_NAME,
        primary_keys=["event_id"],
        transform_fn=transform_to_silver_fact_orders,
)