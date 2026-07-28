from pyspark.sql.functions import (
    input_file_name,
    col,
    to_timestamp,
    struct,
)
from pyspark.sql import DataFrame

def transform_to_bronze(df: DataFrame) -> DataFrame:
    """
    Transform raw customer events into the Bronze schema.
    """
    return (
        df.withColumn(
            "customer",
            struct(
                col("customer.customer_id").alias("customer_id"),
                col("customer.name").alias("name"),
                col("customer.email").alias("email"),
                col("customer.country").alias("country"),
                to_timestamp(col("customer.created_at")).alias("created_at"),
            ),
        )
        .withColumn("_source_file", input_file_name())
        .drop("pipeline_name")
        .select(
            "event_id",
            "event_time",
            "customer",
            "_ingested_at",
            "_source_file",
        )
    )
    
def transform_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform raw customer events into the Bronze schema.
    """
    return (
        df.withColumn(
            "customer",
            struct(
                col("customer.customer_id").alias("customer_id"),
                col("customer.name").alias("name"),
                col("customer.email").alias("email"),
                col("customer.country").alias("country"),
                to_timestamp(col("customer.created_at")).alias("created_at"),
            ),
        )
        .withColumn("_source_file", input_file_name())
        .drop("pipeline_name")
        .select(
            "event_id",
            "event_time",
            "customer",
            "_ingested_at",
            "_source_file",
        )
    )