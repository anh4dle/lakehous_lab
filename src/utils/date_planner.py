from src.core.config import ProcessingConfig, BaseConfig
from src.utils.spark_io import get_max_available_date, get_next_date
from datetime import date, datetime
from src.core.logger import get_logger

logger = get_logger(__name__)


def plan_incremental(
    spark_client, pipeline_config: BaseConfig, sources=None
) -> ProcessingConfig | None:
    pipeline_name = pipeline_config.PIPELINE_NAME
    source_name = pipeline_config.SOURCE_NAME
    source_type = pipeline_config.SOURCE_TYPE
    # Max date from source
    source_max_date = None
    if sources:
        max_dates = []
        for alias, table_name in sources.items():
            print(table_name)
            max_dates.append(
                get_max_available_date(spark_client, source_name=table_name)
            )
        source_max_date = min(max_dates[0], max_dates[1])
    else:
        source_max_date = get_max_available_date(
            spark_client, source_name=source_name, source_type=source_type
        )
    if source_max_date is None:
        logger.info("Source table contains no data: %s", source_name)
        return None
    # Get next date to process
    next_start_date = get_next_date(spark_client, pipeline_name)
    start = datetime.strptime(next_start_date, "%Y-%m-%d").date()
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
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    return ProcessingConfig(
        mode="backfill",
        start_date=start_date,
        end_date=end_date,
    )
