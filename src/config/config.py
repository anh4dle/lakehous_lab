from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

BASE_INPUT_PATH = "s3a://warehouse/raw/"

REGISTRY_TABLE = "iceberg.metadata.ingestion_registry"

CUSTOMER_BRONZE_PIPELINE_NAME = "customer_raw_to_bronze"
BRONZE_CUSTOMER_TABLE = "iceberg.bronze.fact_customer_events"

ORDER_BRONZE_PIPELINE_NAME = "order_raw_to_bronze"
BRONZE_ORDER_TABLE = "iceberg.bronze.fact_orders_events"


CUSTOMER_SILVER_PIPELINE_NAME = "customer_bronze_to_silver"
SILVER_CUSTOMER_TABLE = "iceberg.silver.dim_customer"

ORDER_SILVER_PIPELINE_NAME = "order_bronze_to_silver"
SILVER_ORDER_TABLE = "iceberg.silver.fact_orders"

GOLD_PIPELINE_NAME = "silver_to_gold"
GOLD_TABLE = "iceberg.gold.daily_orders_by_country"
