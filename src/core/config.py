from dataclasses import dataclass
from datetime import date
from typing import Literal
from pathlib import Path
from src.utils.transform_script import (
    transform_to_bronze_customer_events,
    transform_to_bronze_orders_events,
    transform_to_silver_dim_customer,
    transform_to_silver_fact_orders,
    transform_to_gold_daily_orders_by_country,
)
from typing import Callable, Optional
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# --- Global Storage & Metadata ---
BASE_SOURCE_NAME = "s3a://warehouse/raw/"
REGISTRY_TABLE = "iceberg.metadata.ingestion_registry"

RUN_MODE = "local"


@dataclass(frozen=True)
class BaseConfig:
    PIPELINE_NAME: str = ""
    TABLE_NAME: str = ""
    SOURCE_TYPE: str = "iceberg"
    PRIMARY_KEYS: tuple = ()
    TRANSFORM_FN: Optional[Callable] = None
    SOURCE_NAME: str = ""

# --- Bronze Layer Contracts ---
@dataclass(frozen=True)
class CustomerBronzeConfig(BaseConfig):
    PIPELINE_NAME: str = "customer_raw_to_bronze"
    TABLE_NAME: str = "iceberg.bronze.fact_customer_events"
    SOURCE_NAME: str = f"{BASE_SOURCE_NAME}customers/"
    SOURCE_TYPE: str = "raw"
    TRANSFORM_FN: callable = transform_to_bronze_customer_events
    PRIMARY_KEYS: tuple = ("event_id",)

@dataclass(frozen=True)
class OrderBronzeConfig(BaseConfig):
    PIPELINE_NAME: str = "order_raw_to_bronze"
    TABLE_NAME: str = "iceberg.bronze.fact_orders_events"
    SOURCE_NAME: str = f"{BASE_SOURCE_NAME}orders/"
    SOURCE_TYPE: str = "raw"
    TRANSFORM_FN: callable = transform_to_bronze_orders_events
    PRIMARY_KEYS: tuple = ("event_id",)


# --- Silver Layer Contracts ---
@dataclass(frozen=True)
class CustomerSilverConfig(BaseConfig):
    PIPELINE_NAME: str = "customer_bronze_to_silver"
    TABLE_NAME: str = "iceberg.silver.dim_customer"
    SOURCE_NAME: str = "iceberg.bronze.fact_customer_events"
    SOURCE_TYPE: str = "iceberg"
    TRANSFORM_FN: callable = transform_to_silver_dim_customer
    PRIMARY_KEYS: tuple = ("event_id",)


@dataclass(frozen=True)
class OrderSilverConfig(BaseConfig):
    PIPELINE_NAME: str = "order_bronze_to_silver"
    TABLE_NAME: str = "iceberg.silver.fact_orders"
    SOURCE_NAME: str = "iceberg.bronze.fact_orders_events"
    SOURCE_TYPE: str = "iceberg"
    TRANSFORM_FN: callable = transform_to_silver_fact_orders
    PRIMARY_KEYS: tuple = ("event_id",)

# --- Gold Layer Contracts ---
@dataclass(frozen=True)
class DailyOrdersGoldConfig(BaseConfig):
    PIPELINE_NAME: str = "silver_to_gold"
    TABLE_NAME: str = "iceberg.gold.daily_orders_by_country"
    PRIMARY_KEYS: tuple = ("report_date", "country")
    SOURCE_TYPE: str = "iceberg"
    TRANSFORM_FN: callable = transform_to_gold_daily_orders_by_country
    
    
@dataclass(frozen=True)
class ProcessingConfig:
    mode: Literal["incremental", "backfill"]
    start_date: date
    end_date: date

    def __post_init__(self):
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")