from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    struct,
    count,
    countDistinct,
    current_timestamp,
    date_trunc,
    sum as _sum,
    lit,
    to_timestamp,
    row_number,
    when
)
from datetime import datetime, timezone


# =====================================================================
# 1. BRONZE TRANSFORMATIONS
# =====================================================================
def get_current_utc_string() -> str:
    """Generates a static ISO timestamp string in Python runtime."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def transform_to_bronze_customer_events(raw_df: DataFrame) -> DataFrame:
    """
    Transforms raw JSON input into iceberg.bronze.fact_customer_events.
    """
    execution_time = get_current_utc_string()

    raw_df = raw_df.select(
        col("event_id"),
        to_timestamp(col("event_time")).alias("event_time"),
        col("customer"),  # Struct <customer_id, name, email, country>
        to_timestamp(lit(execution_time)).alias("_ingested_at"),
    )

    return raw_df.select(
        col("event_id"),
        col("event_time"),
        struct(
            col("customer.customer_id").alias("customer_id"),
            col("customer.name").alias("name"),
            col("customer.email").alias("email"),
            col("customer.country").alias("country"),
            col("customer.created_at").alias("created_at"),
            col("customer.device_id").alias("device_id"),
            col("customer.tier").alias("tier")
        ).alias("customer"),
        col("_ingested_at"),
    )


def transform_to_bronze_orders_events(raw_df: DataFrame) -> DataFrame:
    """
    Transforms raw JSON input into iceberg.bronze.fact_orders_events.
    """

    execution_time = get_current_utc_string()

    return raw_df.select(
        col("event_id"),
        to_timestamp(col("event_time")).alias("event_time"),
        struct(
            col("order.order_id").cast("string").alias("order_id"),
            col("order.customer_id").cast("string").alias("customer_id"),
            col("order.amount").cast("float").alias("amount"),
            col("order.status").cast("string").alias("status"),
            col("order.items").cast("int").alias("items"),
            col("order.created_at").cast("string").alias("created_at"),
            col("order.session_id").cast("string").alias("session_id"),
            col("order.promo_code").cast("string").alias("promo_code")
        ).alias("order"),
        to_timestamp(lit(execution_time)).alias("_ingested_at"),
    )


# =====================================================================
# 2. SILVER TRANSFORMATIONS
# =====================================================================


def transform_to_silver_dim_customer(bronze_customer_df: DataFrame) -> DataFrame:
    """
    Flattens bronze customer events into iceberg.silver.dim_customer.
    Matches updated DDL: event_id, event_time, customer_id, name, email, country, _ingested_at.
    """
    return bronze_customer_df.select(
        col("event_id"),
        col("event_time"),
        col("customer.customer_id").alias("customer_id"),
        col("customer.name").alias("name"),
        col("customer.email").alias("email"),
        col("customer.country").alias("country"),
        col("customer.device_id").alias("device_id"),
        col("customer.tier").alias("tier"),
        current_timestamp().alias("_ingested_at"),
    )


def transform_to_silver_fact_orders(bronze_orders_df: DataFrame) -> DataFrame:
    """
    Flattens bronze order events into iceberg.silver.fact_orders.
    Casts amount to DECIMAL(18,2) to match table contract.
    """
    return bronze_orders_df.select(
        col("event_id"),
        col("event_time"),
        col("order.customer_id").alias("customer_id"),
        col("order.order_id").alias("order_id"),
        col("order.amount").cast("decimal(18,2)").alias("amount"),
        col("order.status").alias("status"),
        col("order.items").alias("items"),
        col("order.session_id").cast("string").alias("session_id"),
        col("order.promo_code").cast("string").alias("promo_code"),
        current_timestamp().alias("_ingested_at"),
    )


# =====================================================================
# 3. GOLD TRANSFORMATIONS
# =====================================================================


def transform_to_gold_daily_orders_by_country(
    silver_orders_df: DataFrame, silver_customers_df: DataFrame
) -> DataFrame:
    """
    Joins Silver Orders and Silver Customers on customer_id to calculate
    daily metrics per country for iceberg.gold.daily_orders_by_country.
    """
    # 1. Truncate event_time to DATE for report_date
    orders_with_date = silver_orders_df.withColumn(
        "report_date", date_trunc("day", col("event_time")).cast("date")
    )

    # 2. Join Orders with Customers to map customer_id -> country
    # Note: Select only necessary columns from customer to avoid duplicate event_id/event_time columns
    window_spec = Window.partitionBy("customer_id").orderBy(col("event_time").desc())
    customers_lookup = (
        silver_customers_df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .select("customer_id", "country")
    )
    # customers_lookup = silver_customers_df.select("customer_id", "country").distinct()

    joined_df = orders_with_date.join(customers_lookup, on="customer_id", how="inner")

    # 3. Aggregate daily metrics
    gold_df = (
        joined_df.groupBy("report_date", "country")
        .agg(
            count("order_id").alias("order_count"),
            _sum("amount").cast("decimal(18,2)").alias("total_amount"),
            (_sum("amount") / count("order_id"))
            .cast("decimal(18,2)")
            .alias("avg_order_value"),
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
