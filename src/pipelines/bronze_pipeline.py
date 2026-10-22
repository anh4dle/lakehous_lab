from src.core.config import CustomerBronzeConfig, OrderBronzeConfig
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.client.spark_client import SparkClient
from src.utils.date_planner import plan_incremental, plan_backfill
from src.core.logger import get_logger
import argparse

logger = get_logger(__name__)


def run_bronze_pipeline(**kwargs):
    spark_client = SparkClient()
    run_mode = kwargs.get("mode")
    plan = None
    plan_order = None
    # python -m src.pipelines.bronze_pipeline --mode backfill --start-date 2026-05-01 --end-date 2026-05-10
    if run_mode == "backfill":
        plan = plan_backfill(
            start_date=kwargs.get("start_date"),
            end_date=kwargs.get("end_date"),
        )
    else:
        plan = plan_incremental(
            spark_client=spark_client,
            pipeline_config=CustomerBronzeConfig,
        )
        plan_order = plan_incremental(
            spark_client=spark_client,
            pipeline_config=OrderBronzeConfig,
        )

    # Process Customer Bronze
    runner = LakehousePipelineRunner(spark_client)
    runner.run_single_source_pipeline(
        pipeline_config=CustomerBronzeConfig,
        date_plan=plan,
    )

    # Process Order Bronze
    runner.run_single_source_pipeline(
        pipeline_config=OrderBronzeConfig,
        date_plan=plan_order if run_mode == "incremental" else plan,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Bronze Pipeline")
    parser.add_argument(
        "--mode", default="incremental", choices=["incremental", "backfill"]
    )
    parser.add_argument("--start-date", help="Format: YYYY-MM-DD")
    parser.add_argument("--end-date", help="Format: YYYY-MM-DD")
    parsed_args = vars(parser.parse_args())

    run_bronze_pipeline(**parsed_args)
