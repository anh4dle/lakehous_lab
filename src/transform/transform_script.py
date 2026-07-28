from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    current_timestamp,
    date_trunc,
    input_file_name,
    lit,
    sum as _sum,
    to_timestamp,
)
# =====================================================================
# BRONZE LAYER TRANSFORMATIONS
# =====================================================================

def transform_customer_events_to_bronze(df: DataFrame) -> DataFrame:
    """
    Transforms raw ingested customer events into iceberg.bronze.fact_customer_events.
    """
    return df.select(
        col("event_id"),
        to_timestamp(col("event_time")).alias("event_time"),
        col("customer"),  # Struct <customer_id, name, email, country>
        current_timestamp().alias("_ingested_at"),
        input_file_name().alias("_source_file"),
    )


def transform_order_events_to_bronze(df: DataFrame) -> DataFrame:
    """
    Transforms raw ingested order events into iceberg.bronze.fact_orders_events.
    """
    return df.select(
        col("event_id"),
        to_timestamp(col("event_time")).alias("event_time"),
        col("order_detail"),  # Struct <customer_id, order_id, amount, status, items>
        current_timestamp().alias("_ingested_at"),
        input_file_name().alias("_source_file"),
    )


# =====================================================================
# SILVER LAYER TRANSFORMATIONS
# =====================================================================

def transform_to_silver_dim_customer(bronze_customer_df: DataFrame) -> DataFrame:
    """
    Flattens bronze customer events into iceberg.silver.dim_customer.
    """
    return bronze_customer_df.select(
        col("customer.customer_id").alias("customer_id"),
        col("customer.name").alias("name"),
        col("customer.email").alias("email"),
        col("customer.country").alias("country"),
        current_timestamp().alias("_ingested_at"),
        col("event_id").alias("_source_event_id"),
        lit(True).alias("_is_current"),  # Required by DDL PARTITIONED BY (_is_current)
    )


def transform_to_silver_fact_orders(bronze_orders_df: DataFrame) -> DataFrame:
    """
    Flattens bronze order events into iceberg.silver.fact_orders with type casting.
    """
    return bronze_orders_df.select(
        col("event_id"),
        col("event_time"),
        col("order_detail.customer_id").alias("customer_id"),
        col("order_detail.order_id").alias("order_id"),
        col("order_detail.amount").cast("decimal(18,2)").alias("amount"),
        col("order_detail.status").alias("status"),
        col("order_detail.items").alias("items"),
        current_timestamp().alias("_ingested_at"),
    )

# =====================================================================
# GOLD LAYER TRANSFORMATIONS
# =====================================================================

def transform_to_gold_daily_orders_by_country(
    silver_orders_df: DataFrame, silver_customers_df: DataFrame
) -> DataFrame:
    """
    Joins Silver Orders and Silver Customers to build the Gold Data Mart:
    iceberg.gold.daily_orders_by_country.
    """
    # 1. Truncate event_time to DATE for daily aggregation
    orders_with_date = silver_orders_df.withColumn(
        "report_date", date_trunc("day", col("event_time")).cast("date")
    )

    # 2. Join Orders with Customers on customer_id to get country
    joined_df = orders_with_date.join(
        silver_customers_df, on="customer_id", how="inner"
    )

    # 3. Aggregate metrics by report_date and country
    gold_df = (
        joined_df.groupBy("report_date", "country")
        .agg(
            count("order_id").alias("order_count"),
            _sum("amount").cast("decimal(18,2)").alias("total_amount"),
            (_sum("amount") / count("order_id")).cast("decimal(18,2)").alias("avg_order_value"),
            countDistinct("customer_id").alias("distinct_customers"),
        )
        .select(
            "report_date",
            "country",
            "order_count",
            "total_amount",
            "avg_order_value",
            "distinct_customers",
        )
    )

    return gold_df
