from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

BASE_INPUT_PATH = "s3a://warehouse/raw/customers"
TABLE_FULL_NAME = "iceberg.bronze.fact_customer_events"
REGISTRY_TABLE = "iceberg.metadata.ingestion_registry"

CUSTOMER_BRONZE_PIPELINE_NAME = "customer_raw_to_bronze"
CUSTOMER_SILVER_PIPELINE_NAME = "customer_bronze_to_silver"

ORDER_BRONZE_PIPELINE_NAME = "order_raw_to_bronze"
ORDER_SILVER_PIPELINE_NAME = "order_bronze_to_silver"

GOLD_PIPELINE_NAME = "silver_to_gold"
