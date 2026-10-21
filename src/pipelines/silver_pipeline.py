from src.core.config import (
    CustomerSilverConfig,
    OrderSilverConfig,
)
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.client.spark_client import SparkClient
from src.utils.date_planner import plan_incremental, plan_backfill
import argparse


def run_silver_pipeline(**kwargs):
    spark_client = SparkClient()
    plan = None
    plan_order = None
    run_mode = kwargs.get("mode")
    # python -m src.pipelines.bronze_pipeline --mode backfill --start-date 2026-07-01 --end-date 2026-07-07
    if run_mode == "backfill":
        plan = plan_backfill(
            start_date=kwargs.get("start_date"),
            end_date=kwargs.get("end_date"),
        )
    else:
        plan = plan_incremental(
            spark_client=spark_client,
            pipeline_config=CustomerSilverConfig,
        )
        plan_order = plan_incremental(
            spark_client=spark_client,
            pipeline_config=OrderSilverConfig,
        )
    runner = LakehousePipelineRunner(spark_client)
    runner.run_single_source_pipeline(
        pipeline_config=CustomerSilverConfig,
        date_plan=plan,
    )

    runner.run_single_source_pipeline(
        pipeline_config=OrderSilverConfig,
        date_plan=plan_order if run_mode == "incremental" else plan,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Silver Pipeline")
    parser.add_argument(
        "--mode", default="incremental", choices=["incremental", "backfill"]
    )
    parser.add_argument("--start-date", help="Format: YYYY-MM-DD")
    parser.add_argument("--end-date", help="Format: YYYY-MM-DD")
    parsed_args = vars(parser.parse_args())

    run_silver_pipeline(**parsed_args)
