from src.client.spark_client import SparkClient
from src.core.config import REGISTRY_TABLE, OrderSilverConfig, OrderBronzeConfig, DailyOrdersGoldConfig

client = SparkClient()
spark = client.spark
# table = DailyOrdersGoldConfig.TABLE_NAME
table = "iceberg.metadata.ingestion_registry"
df = spark.sql(f"""
    select * from {table}
""")

df.show()
