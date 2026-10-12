# `contracts.py`
import pandera.pyspark as pa
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


def validate_dataframe(df, table_name: str):
    """Validate a Spark DataFrame against its registered contract."""

    contract = get_contract(table_name)
    return contract.validate(df)

