from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# --- Global Storage & Metadata ---
BASE_INPUT_PATH = "s3a://warehouse/raw/"
REGISTRY_TABLE = "iceberg.metadata.ingestion_registry"


# --- Bronze Layer Contracts ---
@dataclass(frozen=True)
class CustomerBronzeConfig:
    PIPELINE_NAME: str = "customer_raw_to_bronze"
    TABLE_NAME: str = "iceberg.bronze.fact_customer_events"
    INPUT_PATH: str = f"{BASE_INPUT_PATH}customers/"
    SOURCE_TYPE: str = "raw"


@dataclass(frozen=True)
class OrderBronzeConfig:
    PIPELINE_NAME: str = "order_raw_to_bronze"
    TABLE_NAME: str = "iceberg.bronze.fact_orders_events"
    INPUT_PATH: str = f"{BASE_INPUT_PATH}orders/"
    SOURCE_TYPE: str = "raw"


# --- Silver Layer Contracts ---
@dataclass(frozen=True)
class CustomerSilverConfig:
    PIPELINE_NAME: str = "customer_bronze_to_silver"
    TABLE_NAME: str = "iceberg.silver.dim_customer"
    SOURCE_TYPE: str = "iceberg"


@dataclass(frozen=True)
class OrderSilverConfig:
    PIPELINE_NAME: str = "order_bronze_to_silver"
    TABLE_NAME: str = "iceberg.silver.fact_orders"
    SOURCE_TYPE: str = "iceberg"


# --- Gold Layer Contracts ---
@dataclass(frozen=True)
class DailyOrdersGoldConfig:
    PIPELINE_NAME: str = "silver_to_gold"
    TABLE_NAME: str = "iceberg.gold.daily_orders_by_country"
    PRIMARY_KEYS: tuple = ("report_date", "country")
    SOURCE_TYPE: str = "iceberg"
