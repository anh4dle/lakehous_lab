from src.core.config import CustomerBronzeConfig, OrderBronzeConfig
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.client.spark_client import SparkClient
from src.utils.date_planner import plan_incremental, plan_backfill
from src.core.logger import get_logger
logger = get_logger(__name__)

def run_bronze_pipeline(**args):
    spark_client = SparkClient()
    
    plan = plan_incremental(
            spark_client=spark_client,
            pipeline_config=CustomerBronzeConfig,
        )
    # if args["mode"] == "backfill":
    #     plan = plan_backfill(
    #         start_date=args["start_date"],
    #         end_date=args["end_date"],
    #     )

    # Process Customer Bronze
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
