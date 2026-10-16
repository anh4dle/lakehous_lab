# `contracts.py`
from pandera.pyspark import Column, DataFrameSchema
from pandera import Check  # Import Pandera Check
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

CUSTOMER_STRUCT = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

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

ORDER_STRUCT = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("status", StringType(), True),
        StructField("items", IntegerType(), True),
        StructField("created_at", StringType(), True),
    ]
)

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
        "avg_order_value": column(DecimalType(18, 2), nullable=True, checks=Check.ge(0.0)),
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
    Validates a DataFrame against a Pandera contract (checking both schema and value constraints).
    Splits it into (clean_df, quarantine_df) if validation fails.
    """
    contract = get_contract(table_name)
    
    if df is None:
        logger.info("Empty dataframe passed to validate_and_split_dataframe")
        return df, None

    try:
        # lazy=True ensures Pandera evaluates all rows for both schema and value checks
        validated_df = contract.validate(df, lazy=True)
        
        # If no exception is raised, everything is clean
        empty_dlq = df.limit(0)
        return validated_df, empty_dlq

    except pandera.errors.SchemaErrors as err:
        logger.warning(
            "Validation failed (Schema or Value violation) for table %s. Isolating bad rows...", 
            table_name
        )
        
        # Extract the failure cases dataframe generated by Pandera
        failure_cases = err.failure_cases
        return handle_row_level_quarantine(df, table_name, failure_cases)


def handle_row_level_quarantine(
    df: DataFrame, table_name: str, failure_cases: DataFrame = None
) -> tuple[DataFrame, DataFrame]:
    """
    Isolates bad rows using Pandera's failure metadata or fallback rules.
    """
    # If Pandera captured specific row failures, we can isolate them using the failure index
    if failure_cases is not None and not failure_cases.rdd.isEmpty() and "index" in failure_cases.columns:
        failed_indices = failure_cases.select("index").distinct()
        
        # Assuming your dataframe rows have an index identifier matching Pandera's failure report
        quarantine_df = df.join(failed_indices, on="index", how="inner")
        clean_df = df.join(failed_indices, on="index", how="left_anti")
        return clean_df, quarantine_df

    # Fallback business rule filtering if index tracking isn't mapped
    if "fact_orders" in table_name:
        clean_df = df.filter(
            (df.amount >= 0.0)
            & (df.customer_id.isNotNull())
            & (df.order_id.isNotNull())
        )
        quarantine_df = df.subtract(clean_df)
        return clean_df, quarantine_df

    elif "daily_orders_by_country" in table_name:
        clean_df = df.filter(
            df.event_id.isNotNull()
            & (df.total_amount >= 0.0)
            & (df.avg_order_value >= 0.0)
        )
        quarantine_df = df.subtract(clean_df)
        return clean_df, quarantine_df

    # Default fallback
    return df, df.limit(0)