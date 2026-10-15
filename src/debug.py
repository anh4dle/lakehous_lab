from src.client.spark_client import SparkClient


client = SparkClient()
spark = client.spark
table = "iceberg.metadata.ingestion_registry"
df = spark.sql(f"""
    select * from {table} where pipeline_name = 'customer_raw_to_bronze' order by next_date desc
""")

df.show()
