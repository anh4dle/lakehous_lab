from src.core.config import (
    CustomerSilverConfig,
    OrderSilverConfig,
)
from src.pipelines.pipeline_runner import LakehousePipelineRunner
from src.client.spark_client import SparkClient
from src.utils.date_planner import plan_incremental

def run_silver_pipeline():
    spark_client = SparkClient()
    plan = plan_incremental(
            spark_client=spark_client,
            pipeline_config=CustomerSilverConfig,
        )
    runner = LakehousePipelineRunner(spark_client)
    runner.run_single_source_pipeline(
        pipeline_config=CustomerSilverConfig,
        date_plan=plan,
    )

    runner.run_single_source_pipeline(
        pipeline_config=OrderSilverConfig,
        date_plan=plan,
    )


if __name__ == "__main__":
    run_silver_pipeline()
