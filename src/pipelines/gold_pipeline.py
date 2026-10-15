from src.core.config import (
    CustomerSilverConfig,
    DailyOrdersGoldConfig,
    OrderSilverConfig,
)
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.client.spark_client import SparkClient
from src.utils.date_planner import plan_incremental, plan_backfill

def run_gold_pipeline():
    spark_client = SparkClient()
    GOLD_SOURCES = {
        "silver_orders_df": OrderSilverConfig.TABLE_NAME,
        "silver_customers_df": CustomerSilverConfig.TABLE_NAME,
    }
    plan = plan_incremental(
            spark_client=SparkClient(),
            pipeline_config=DailyOrdersGoldConfig,
            sources=GOLD_SOURCES,
        )
    runner = LakehousePipelineRunner(spark_client)
    runner.run_multi_source_pipeline(
        pipeline_config=DailyOrdersGoldConfig,
        sources=GOLD_SOURCES,
        plan=plan,
    )


if __name__ == "__main__":
    run_gold_pipeline()
