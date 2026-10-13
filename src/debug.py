from src.client.spark_client import SparkClient


client = SparkClient()
spark = client.spark
table = "iceberg.metadata.ingestion_registry"
spark.sql(f"""
    ALTER TABLE {table}
    RENAME COLUMN last_processed_date TO next_date
""")
df = spark.table(table)

df.show()
