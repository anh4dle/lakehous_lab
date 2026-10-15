
from src.core.config import ProcessingConfig, BaseConfig
from src.utils.spark_io import get_max_available_date, get_next_date
from datetime import date, timedelta, datetime
from src.core.logger import get_logger

logger = get_logger(__name__)

def plan_incremental(
    spark_client,
    source_max_date: date,
    pipeline_config: BaseConfig,
) -> ProcessingConfig | None:
    pipeline_name = pipeline_config.PIPELINE_NAME
    input_path = pipeline_config.INPUT_PATH
    source_type = pipeline_config.SOURCE_TYPE
    #Max date from source
    source_max_date = get_max_available_date(
        spark_client, input_path, source_type
    )

    if source_max_date is None:
        logger.info("Source table contains no data: %s", input_path)
        return
    #Next date to process
    last_date_str = get_next_date(spark_client, pipeline_name)
    last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")
    start = last_dt + timedelta(days=1)

    if start > source_max_date:
        return None

    return ProcessingConfig(
        mode="incremental",
        start_date=start,
        end_date=source_max_date,
    )

def plan_backfill(
    start_date: date,
    end_date: date,
) -> ProcessingConfig:
    return ProcessingConfig(
        mode="backfill",
        start_date=start_date,
        end_date=end_date,
    )