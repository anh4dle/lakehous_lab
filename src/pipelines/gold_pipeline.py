from src.config.config import (
    CustomerSilverConfig,
    DailyOrdersGoldConfig,
    OrderSilverConfig,
)
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.transform.transform_script import transform_to_gold_daily_orders_by_country
from src.client.spark_client import SparkClient

def run_gold_pipeline():
    spark_client = SparkClient()
    runner = LakehousePipelineRunner(spark_client)
    runner.run_multi_source_pipeline(
        pipeline_name=DailyOrdersGoldConfig.PIPELINE_NAME,
        sources={
            "silver_orders_df": OrderSilverConfig.TABLE_NAME,
            "silver_customers_df": CustomerSilverConfig.TABLE_NAME,
        },
        target_table=DailyOrdersGoldConfig.TABLE_NAME,
        primary_keys=list(DailyOrdersGoldConfig.PRIMARY_KEYS),
        transform_fn=transform_to_gold_daily_orders_by_country,
    )


if __name__ == "__main__":
    run_gold_pipeline()