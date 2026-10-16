from src.core.config import CustomerBronzeConfig, OrderBronzeConfig
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.client.spark_client import SparkClient
from src.utils.date_planner import plan_incremental, plan_backfill

def run_bronze_pipeline(**args):
    plan = plan_incremental(
            last_processed_date=args["last_processed_date"],
            source_max_date=args["source_max_date"],
        )
    if args["mode"] == "backfill":
        plan = plan_backfill(
            start_date=args["start_date"],
            end_date=args["end_date"],
        )

    # Process Customer Bronze
    spark_client = SparkClient()
    runner = LakehousePipelineRunner(spark_client)
    runner.run_single_source_pipeline(
        pipeline_config=CustomerBronzeConfig,
        date_plan=plan,
    )

    # Process Order Bronze
    runner.run_single_source_pipeline(
        pipeline_config=OrderBronzeConfig,
        date_plan=plan,
    )


if __name__ == "__main__":
    run_bronze_pipeline()
