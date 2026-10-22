# `contracts.py`
from pandera.pyspark import Column, DataFrameSchema
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pandera import Check  # Import Pandera Check
from pyspark.sql.types import (
    StringType,
    TimestampType,
    IntegerType,
    LongType,
    DecimalType,
    DateType,
)
from src.core.logger import get_logger

logger = get_logger(__name__)


# ============================================================
# Shared helpers
# ============================================================


def nullable_column(dtype):
    """Create a nullable column, matching the supplied DDL."""
    return Column(dtype=dtype, nullable=True)


# ============================================================
# BRONZE: fact_customer_events
# Table: iceberg.bronze.fact_customer_events
# ============================================================


FACT_CUSTOMER_EVENTS = DataFrameSchema(
    {
        "event_id": nullable_column(StringType()),
        "event_time": nullable_column(TimestampType()),
        "_ingested_at": nullable_column(TimestampType()),
    },
    strict=True,
    coerce=False,
)


# ============================================================
# BRONZE: fact_orders_events
# Table: iceberg.bronze.fact_orders_events
# ============================================================

FACT_ORDERS_EVENTS = DataFrameSchema(
    {
        "event_id": nullable_column(StringType()),
        "event_time": nullable_column(TimestampType()),
        "_ingested_at": nullable_column(TimestampType()),
    },
    strict=True,
    coerce=False,
)


# ============================================================
# SILVER: dim_customer
# Table: iceberg.silver.dim_customer
# ============================================================

DIM_CUSTOMER = DataFrameSchema(
    {
        "event_id": nullable_column(StringType()),
        "event_time": nullable_column(TimestampType()),
        "customer_id": nullable_column(StringType()),
        "name": nullable_column(StringType()),
        "email": nullable_column(StringType()),
        "country": nullable_column(StringType()),
        "_ingested_at": nullable_column(TimestampType()),
    },
    strict=True,
    coerce=False,
)


# ============================================================
# Shared helpers
# ============================================================


def column(dtype, nullable=True, checks=None):
    """Create a column with optional value checks."""
    return Column(dtype=dtype, nullable=nullable, checks=checks)


# ============================================================
# SILVER: fact_orders
# Table: iceberg.silver.fact_orders
# ============================================================

FACT_ORDERS = DataFrameSchema(
    {
        "event_id": column(StringType(), nullable=True),
        "event_time": column(TimestampType(), nullable=True),
        # Add value constraints directly into the schema checks
        "customer_id": column(StringType(), nullable=False),
        "order_id": column(StringType(), nullable=False),
        "amount": column(DecimalType(18, 2), nullable=True, checks=Check.ge(0.0)),
        "status": column(StringType(), nullable=True),
        "items": column(IntegerType(), nullable=True, checks=Check.ge(0)),
        "_ingested_at": column(TimestampType(), nullable=True),
    },
    strict=True,
    coerce=False,
)


# ============================================================
# GOLD: daily_orders_by_country
# Table: iceberg.gold.daily_orders_by_country
# ============================================================

DAILY_ORDERS_BY_COUNTRY = DataFrameSchema(
    {
        "report_date": column(DateType(), nullable=True),
        "country": column(StringType(), nullable=True),
        "order_count": column(LongType(), nullable=True, checks=Check.ge(0)),
        "total_amount": column(DecimalType(18, 2), nullable=True, checks=Check.ge(0.0)),
        "avg_order_value": column(
            DecimalType(18, 2), nullable=True, checks=Check.ge(0.0)
        ),
        "distinct_customers": column(LongType(), nullable=True, checks=Check.ge(0)),
    },
    strict=True,
    coerce=False,
)


# ============================================================
# Contract registry
# ============================================================

CONTRACTS = {
    "iceberg.bronze.fact_customer_events": FACT_CUSTOMER_EVENTS,
    "iceberg.bronze.fact_orders_events": FACT_ORDERS_EVENTS,
    "iceberg.silver.dim_customer": DIM_CUSTOMER,
    "iceberg.silver.fact_orders": FACT_ORDERS,
    "iceberg.gold.daily_orders_by_country": DAILY_ORDERS_BY_COUNTRY,
}


def get_contract(table_name: str) -> DataFrameSchema:
    """Return a contract by layer.table name."""
    try:
        return CONTRACTS[table_name]
    except KeyError:
        raise ValueError(
            f"Unknown table contract: {table_name!r}. "
            f"Available contracts: {list(CONTRACTS)}"
        ) from None


def validate_and_split_dataframe(
    df: DataFrame, table_name: str
) -> tuple[DataFrame, DataFrame]:
    """
    Validates and splits a DataFrame into (clean_df, quarantine_df)  
    """
    if df is None:
        logger.info("Empty dataframe passed to validate_and_split_dataframe")
        return df, None

    # Define table-specific validation rules as native PySpark boolean expressions
    if "fact_orders" in table_name:
        is_valid = (
            F.col("customer_id").isNotNull()
            & F.col("order_id").isNotNull()
            & (F.col("amount") >= 0.0)
            & (F.col("items") >= 0.0)
        )
    elif "daily_orders_by_country" in table_name:
        is_valid = (
            F.col("total_amount").isNotNull()
            & (F.col("total_amount") >= 0.0)
            & (F.col("avg_order_value") >= 0.0)
        )
    elif "fact_customer_events" in table_name or "fact_orders_events" in table_name:
        is_valid = (
            F.col("event_id").isNotNull()
            & F.col("event_time").isNotNull()
        )
    else:
        logger.warning(
            "No explicit validation rules found for table %s. Defaulting all rows to valid.",
            table_name,
        )
        is_valid = F.lit(True)

    # 1. Add a temporary validation flag column evaluated in a single distributed pass
    flagged_df = df.withColumn("_is_valid", is_valid)

    # 2. Split into clean and quarantine streams using lazy pointers (zero shuffles)
    clean_df = flagged_df.filter(F.col("_is_valid") == True).drop("_is_valid")
    quarantine_df = flagged_df.filter(F.col("_is_valid") == False).drop("_is_valid")

    logger.info(
        "Flag and Split validation completed successfully for table %s.",
        table_name,
    )

    return clean_df, quarantine_df