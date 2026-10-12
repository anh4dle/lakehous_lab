# `contracts.py`
from pandera.pyspark import Column, DataFrameSchema
from pyspark.sql.types import (
    StringType,
    TimestampType,
    StructType,
    StructField,
    FloatType,
    IntegerType,
    LongType,
    DecimalType,
    DateType,
)
from pyspark.sql import DataFrame
import pandera.errors
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

CUSTOMER_STRUCT = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", StringType(), True),
])

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

ORDER_STRUCT = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("status", StringType(), True),
    StructField("items", IntegerType(), True),
    StructField("created_at", StringType(), True),
])

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
# SILVER: fact_orders
# Table: iceberg.silver.fact_orders
# ============================================================

FACT_ORDERS = DataFrameSchema(
    {
        "event_id": nullable_column(StringType()),
        "event_time": nullable_column(TimestampType()),
        "customer_id": nullable_column(StringType()),
        "order_id": nullable_column(StringType()),
        "amount": nullable_column(DecimalType(18, 2)),
        "status": nullable_column(StringType()),
        "items": nullable_column(IntegerType()),
        "_ingested_at": nullable_column(TimestampType()),
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
        "report_date": nullable_column(DateType()),
        "country": nullable_column(StringType()),
        "order_count": nullable_column(LongType()),
        "total_amount": nullable_column(DecimalType(18, 2)),
        "avg_order_value": nullable_column(DecimalType(18, 2)),
        "distinct_customers": nullable_column(LongType()),
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

def validate_and_split_dataframe(df: DataFrame, table_name: str) -> tuple[DataFrame, DataFrame]:
    """
    Validates a DataFrame against a Pandera contract. 
    Splits it into (clean_df, quarantine_df) if validation fails.
    """
    contract = get_contract(table_name)
    try:
        # FIX: Pass df positionally instead of using check_obj=df
        if df is None:
            logger.info("Empty dataframe passed to validate_and_split_dataframe")
            return df, None
        validated_df = contract.validate(df, lazy=True)
        
        # If successful, return the original df as clean, and an empty dataframe for DLQ
        empty_dlq = df.limit(0)
        return validated_df, empty_dlq

    except pandera.errors.SchemaErrors as err:
        logger.warning("Schema validation failed for table %s. Isolating bad rows...", table_name)
        failure_cases = err.failure_cases
        return handle_row_level_quarantine(df, table_name)

def handle_row_level_quarantine(df: DataFrame, table_name: str) -> tuple[DataFrame, DataFrame]:
    if "fact_orders" in table_name:
        # Example rule: amount must be >= 0, customer_id and order_id must not be null
        clean_df = df.filter(
            (df.amount >= 0.0) & 
            (df.customer_id.isNotNull()) & 
            (df.order_id.isNotNull())
        )
        quarantine_df = df.subtract(clean_df)
        return clean_df, quarantine_df
        
    elif "daily_orders_by_country" in table_name:
        clean_df = df.filter(df.event_id.isNotNull() & df.customer.customer_id.isNotNull()
                             & (df.total_amount >= 0.0) & (df.avg_order_value >= 0.0))
        quarantine_df = df.subtract(clean_df)
        return clean_df, quarantine_df
        
    # Default fallback: if no specific rule, assume all clean
    return df, df.limit(0)