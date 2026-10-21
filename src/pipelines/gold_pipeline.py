from src.core.config import (
    CustomerSilverConfig,
    DailyOrdersGoldConfig,
    OrderSilverConfig,
)
import argparse
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.client.spark_client import SparkClient
from src.utils.date_planner import plan_incremental, plan_backfill


def run_gold_pipeline(**kwargs):
    spark_client = SparkClient()
    run_mode = kwargs.get("mode")
    GOLD_SOURCES = {
        "silver_orders_df": OrderSilverConfig.TABLE_NAME,
        "silver_customers_df": CustomerSilverConfig.TABLE_NAME,
    }
    if run_mode == "backfill":
        plan = plan_backfill(
            start_date=kwargs.get("start_date"),
            end_date=kwargs.get("end_date"),
        )
    else:
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
    parser = argparse.ArgumentParser(description="Run Gold Pipeline")
    parser.add_argument(
        "--mode", default="incremental", choices=["incremental", "backfill"]
    )
    parser.add_argument("--start-date", help="Format: YYYY-MM-DD")
    parser.add_argument("--end-date", help="Format: YYYY-MM-DD")
    parsed_args = vars(parser.parse_args())

    run_gold_pipeline(**parsed_args)
