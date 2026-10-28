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
from pyspark.storagelevel import StorageLevel

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
    strict=False,
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
    strict=False,
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
        "device_id": nullable_column(StringType()),
        "tier": nullable_column(StringType()),
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
        "session_id": column(StringType()),
        "promo_code": column(StringType()),
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


def handle_missing_columns(df: DataFrame, schema: DataFrameSchema) -> DataFrame:
    """
    Detects missing columns against the contract.
    Halts if required fields are missing; injects NULLs for nullable fields.
    """
    missing = [c for c in schema.columns if c not in df.columns]
    if not missing:
        return df

    # Check if any missing columns are non-nullable (required)
    required_missing = [c for c in missing if not schema.columns[c].nullable]
    if required_missing:
        raise ValueError(f"🚨 Required columns missing from source: {required_missing}")

    # Inject NULLs cast to the expected contract datatype for missing nullable columns
    for c in missing:
        col_type = schema.columns[c].dtype
        df = df.withColumn(c, F.lit(None).cast(col_type))

    logger.warning("⚠️ Columns missing from source, filled with NULL: %s", missing)
    return df


def handle_datatype_drift(df: DataFrame, schema: DataFrameSchema) -> DataFrame:
    """
    Detects datatype drift between incoming DataFrame columns and the contract schema.
    Logs warnings and attempts safe casts to align with contract definitions.
    """
    drift_detected = []

    for col_name, col_config in schema.columns.items():
        if col_name in df.columns:
            incoming_type = df.schema[col_name].dataType
            expected_type = col_config.dtype

            # Compare Spark data type class representations
            if type(incoming_type) != type(expected_type):
                drift_detected.append(
                    {
                        "column": col_name,
                        "incoming": str(incoming_type),
                        "expected": str(expected_type),
                    }
                )
                # Attempt safe cast to contract datatype
                df = df.withColumn(col_name, F.col(col_name).cast(expected_type))

    if drift_detected:
        logger.warning(
            "⚠️ Datatype drift detected and coerced for columns: %s", drift_detected
        )
        raise

    return df


def handle_extra_cols(df: DataFrame, schema: DataFrameSchema) -> DataFrame:
    """
    Handles extra columns based on contract strictness:
    - Bronze (strict=False): Retains contract columns first, followed by extra columns.
    - Silver/Gold (strict=True): Prunes all extra columns to maintain exact schema alignment.
    """
    contract_cols = list(schema.columns.keys())
    extra_cols = [c for c in df.columns if c not in contract_cols]

    if schema.strict:
        # Strict contracts (silver/gold) retain only contract columns
        return df.select(*contract_cols)

    # Non-strict contracts (bronze): contract columns first, followed by extra columns
    return df.select(*contract_cols, *extra_cols)


def validate_values_and_key_constraints(df, table_name):
    # 3. Define table-specific validation rules as native PySpark boolean expressions
    is_valid = F.lit(True)
    if "silver.fact_orders" in table_name:
        is_valid = (
            F.col("customer_id").isNotNull()
            & F.col("order_id").isNotNull()
            & (F.col("amount") >= 0.0)
            & (F.col("items") >= 0.0)
        )
    elif "fact_customer_events" in table_name or "fact_orders_events" in table_name:
        is_valid = F.col("event_id").isNotNull() & F.col("event_time").isNotNull()

    # 4. Use validated_df (not raw df) to add the temporary validation flag
    flagged_df = df.withColumn("_is_valid", is_valid)

    # 5. CRITICAL: Persist to memory/disk to prevent Spark from re-computing the DAG twice during writes
    flagged_df.persist(StorageLevel.MEMORY_AND_DISK)

    # 6. Split into clean and quarantine streams
    clean_df = flagged_df.filter(F.col("_is_valid") == True).drop("_is_valid")
    quarantine_df = flagged_df.filter(F.col("_is_valid") == False).drop("_is_valid")

    logger.info(
        "Flag and Split validation completed successfully for table %s.",
        table_name,
    )

    return clean_df, quarantine_df


def validate_and_split_dataframe(
    df: DataFrame, table_name: str
) -> tuple[DataFrame, DataFrame]:
    """
    Validates schema using Pandera, evaluates row-level business rules,
    and splits into (clean_df, quarantine_df).
    """
    # 1. Early exit check before touching contracts
    if df is None or df.rdd.isEmpty():
        logger.info(
            "Empty dataframe passed to validate_and_split_dataframe for %s", table_name
        )
        return df, None

    # 2. Validate schema and handle contract drift/errors
    contract = get_contract(table_name)
    df = handle_missing_columns(df, contract)
    df = handle_datatype_drift(df, contract)
    df = handle_extra_cols(df, contract)

    try:
        df = contract.validate(df, lazy=False)
    except Exception as e:
        logger.error(
            f"🚨 CRITICAL: Schema contract validation failed for {table_name}. Halting pipeline."
        )
        logger.error(f"Showing schemas {df.schema}")
        raise e  # Fail fast and loud for structural breaks

    clean_df, quarantine_df = validate_values_and_key_constraints(df, table_name)
