from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
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
"""
- This class define the table schemas and the validation mode(strict or not)
- It stores functions to handle 4 cases in schema drift: sources have new columns, source missing columns, data type
mismatch between source and target, data values not adhere to business rules.
"""
# ============================================================
# BRONZE: fact_customer_events
# ============================================================

FACT_CUSTOMER_EVENTS = {
    "strict": False,
    "columns": {
        "event_id": {"dtype": StringType(), "nullable": True},
        "event_time": {"dtype": TimestampType(), "nullable": True},
        "_ingested_at": {"dtype": TimestampType(), "nullable": True},
    }
}

# ============================================================
# BRONZE: fact_orders_events
# ============================================================

FACT_ORDERS_EVENTS = {
    "strict": False,
    "columns": {
        "event_id": {"dtype": StringType(), "nullable": True},
        "event_time": {"dtype": TimestampType(), "nullable": True},
        "_ingested_at": {"dtype": TimestampType(), "nullable": True},
    }
}

# ============================================================
# SILVER: dim_customer
# ============================================================

DIM_CUSTOMER = {
    "strict": True,
    "columns": {
        "event_id": {"dtype": StringType(), "nullable": True},
        "event_time": {"dtype": TimestampType(), "nullable": True},
        "customer_id": {"dtype": StringType(), "nullable": True},
        "name": {"dtype": StringType(), "nullable": True},
        "email": {"dtype": StringType(), "nullable": True},
        "country": {"dtype": StringType(), "nullable": True},
        "device_id": {"dtype": StringType(), "nullable": True},
        "tier": {"dtype": StringType(), "nullable": True},
        "_ingested_at": {"dtype": TimestampType(), "nullable": True},
    }
}

# ============================================================
# SILVER: fact_orders
# ============================================================

FACT_ORDERS = {
    "strict": True,
    "columns": {
        "event_id": {"dtype": StringType(), "nullable": True},
        "event_time": {"dtype": TimestampType(), "nullable": True},
        "customer_id": {"dtype": StringType(), "nullable": False},
        "order_id": {"dtype": StringType(), "nullable": False},
        "amount": {"dtype": DecimalType(18, 2), "nullable": True},
        "status": {"dtype": StringType(), "nullable": True},
        "items": {"dtype": IntegerType(), "nullable": True},
        "session_id": {"dtype": StringType(), "nullable": True},
        "promo_code": {"dtype": StringType(), "nullable": True},
        "_ingested_at": {"dtype": TimestampType(), "nullable": True},
    }
}

# ============================================================
# GOLD: daily_orders_by_country
# ============================================================

DAILY_ORDERS_BY_COUNTRY = {
    "strict": True,
    "columns": {
        "report_date": {"dtype": DateType(), "nullable": True},
        "country": {"dtype": StringType(), "nullable": True},
        "order_count": {"dtype": LongType(), "nullable": True},
        "total_amount": {"dtype": DecimalType(18, 2), "nullable": True},
        "avg_order_value": {"dtype": DecimalType(18, 2), "nullable": True},
        "distinct_customers": {"dtype": LongType(), "nullable": True},
    }
}

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


def get_contract(table_name: str) -> dict:
    """Return a contract definition by layer.table name."""
    try:
        return CONTRACTS[table_name]
    except KeyError:
        raise ValueError(
            f"Unknown table contract: {table_name!r}. "
            f"Available contracts: {list(CONTRACTS)}"
        ) from None


# ============================================================
# Native Validation & Drift Handlers
# ============================================================


def handle_missing_columns(df: DataFrame, contract: dict) -> DataFrame:
    """Detects missing columns against contract; halts pipeline if required, injects NULL if nullable."""
    schema_cols = contract["columns"]
    missing = [c for c in schema_cols if c not in df.columns]
    if not missing:
        return df

    required_missing = [c for c in missing if not schema_cols[c]["nullable"]]
    if required_missing:
        raise ValueError(f"🚨 Required columns missing from source: {required_missing}")

    for c in missing:
        col_type = schema_cols[c]["dtype"]
        df = df.withColumn(c, F.lit(None).cast(col_type))

    logger.warning("⚠️ Columns missing from source, filled with NULL: %s", missing)
    return df

def handle_datatype_drift(df: DataFrame, contract: dict) -> DataFrame:
    """Detects type mismatches against contract and handles safe widening vs dangerous drift."""
    schema_cols = contract["columns"]

    # Rank integer types by width/capacity to evaluate drift safety
    INT_TYPE_RANK = {
        T.ByteType: 1,
        T.ShortType: 2,
        T.IntegerType: 3,
        T.LongType: 4
    }

    for col_name, col_config in schema_cols.items():
        if col_name in df.columns:
            incoming_type = df.schema[col_name].dataType
            expected_type = col_config["dtype"]
            
            incoming_cls = type(incoming_type)
            expected_cls = type(expected_type)

            if incoming_cls != expected_cls:
                # Scenario 1: Both are numeric integer types
                if incoming_cls in INT_TYPE_RANK and expected_cls in INT_TYPE_RANK:
                    # Safe Upcasting: Incoming data is wider than contract expected (e.g., Integer -> Long)
                    if INT_TYPE_RANK[incoming_cls] > INT_TYPE_RANK[expected_cls]:
                        logger.warning(
                            f"⚠️ Safe data widening detected for column '{col_name}': "
                            f"Incoming data is {incoming_cls.__name__} but schema contract expected {expected_cls.__name__}. "
                            f"Proceeding with the wider type."
                        )
                        # No exception raised; we allow the wider incoming type to pass through
                        continue
                    
                    # Dangerous Downcasting: Incoming data is narrower than contract or trying to force wide into narrow
                    else:
                        logger.error(
                            f"❌ Dangerous data narrowing detected for column '{col_name}': "
                            f"Cannot safely fit incoming {incoming_cls.__name__} into expected contract type {expected_cls.__name__}."
                        )
                        raise TypeError(f"Dangerous data drift on column '{col_name}': {incoming_cls.__name__} to {expected_cls.__name__}")
                
                # Scenario 2: Completely incompatible types (e.g., String vs Integer, Struct vs Array)
                else:
                    logger.error(
                        f"❌ Incompatible datatype drift detected for column '{col_name}': "
                        f"Incoming: {incoming_cls.__name__} but expected contract: {expected_cls.__name__}."
                    )
                    raise TypeError(f"Incompatible datatype drift on column '{col_name}'")

    return df


def handle_extra_cols(df: DataFrame, contract: dict) -> DataFrame:
    """Handles extra columns based on strictness (Bronze retains, Silver/Gold prune)."""
    schema_cols = contract["columns"]
    contract_cols = list(schema_cols.keys())
    extra_cols = [c for c in df.columns if c not in contract_cols]

    if contract["strict"]:
        return df.select(*contract_cols)
    
    return df.select(*contract_cols, *extra_cols)


def validate_values_and_key_constraints(df: DataFrame, table_name: str) -> tuple[DataFrame, DataFrame]:
    """Evaluates row-level business rules and splits into clean and quarantine streams."""
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

    flagged_df = df.withColumn("_is_valid", is_valid)
    flagged_df.persist(StorageLevel.MEMORY_AND_DISK)

    clean_df = flagged_df.filter(F.col("_is_valid") == True).drop("_is_valid")
    quarantine_df = flagged_df.filter(F.col("_is_valid") == False).drop("_is_valid")

    logger.info("Flag and Split validation completed successfully for table %s.", table_name)
    return clean_df, quarantine_df


def validate_and_split_dataframe(
    df: DataFrame, table_name: str
) -> tuple[DataFrame, DataFrame]:
    """
    Validates schema using native PySpark definitions, handles missing/drifted columns,
    and splits into (clean_df, quarantine_df).
    """
    if df is None or df.rdd.isEmpty():
        logger.info("Empty dataframe passed to validate_and_split_dataframe for %s", table_name)
        return df, None

    contract = get_contract(table_name)
    
    # Apply native handlers
    df = handle_missing_columns(df, contract)
    df = handle_datatype_drift(df, contract)
    df = handle_extra_cols(df, contract)

    clean_df, quarantine_df = validate_values_and_key_constraints(df, table_name)
    return clean_df, quarantine_df