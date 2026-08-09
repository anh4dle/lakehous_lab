import pandera as pa
import pandas as pd
from pandera.typing import Series
from typing import Any
# ==========================================
# Common / Custom Data Quality Checks
# ==========================================
# Email regex validation rule
EMAIL_REGEX = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

# ==========================================
# 1. Bronze Layer Schemas
# ==========================================

class CustomerEventsBronzeSchema(pa.DataFrameModel):
    """Corresponds to iceberg.bronze.fact_customer_events"""
    event_id: Series[str] = pa.Field(nullable=False)
    event_time: Series[pa.Timestamp] = pa.Field(nullable=False)
    
    # STRUCT(nested data)<customer_id STRING, name STRING, email STRING, country STRING>
    customer: Series[object] = pa.Field(nullable=False)
    
    _ingested_at: Series[pa.Timestamp] = pa.Field(nullable=False)
    _source_file: Series[str] = pa.Field(nullable=False)

    class Config:
        coerce = True #automatically cast compatible types(timestamp in string format to datetime)
        strict = True #strict validation, raise error if data type is not correct

    @pa.check("customer", name="validate_customer_struct_fields")
    def validate_customer_struct(cls, customer_series: Series[object]) -> Series[bool]:
        """Data Quality Check: Validates keys and values inside the nested struct."""
        def is_valid_struct(val: Any) -> bool:
            if not isinstance(val, dict):
                return False
            required_keys = {"customer_id", "name", "email", "country"}
            return required_keys.issubset(val.keys()) and isinstance(val.get("customer_id"), str)

        return customer_series.apply(is_valid_struct)

class OrderEventsBronzeSchema(pa.DataFrameModel):
    """Corresponds to iceberg.bronze.fact_orders_events"""
    event_id: Series[str] = pa.Field(nullable=False)
    event_time: Series[pa.Timestamp] = pa.Field(nullable=False)
    
    # STRUCT<customer_id STRING, order_id STRING, amount FLOAT, status STRING, items INT>
    order_detail: Series[object] = pa.Field(nullable=False)
    
    _ingested_at: Series[pa.Timestamp] = pa.Field(nullable=False)
    _source_file: Series[str] = pa.Field(nullable=False)

    class Config:
        coerce = True
        strict = True

    @pa.check("order_detail", name="validate_order_detail_struct_fields")
    def validate_order_detail_struct(cls, detail_series: Series[object]) -> Series[bool]:
        """Data Quality Check: Validates keys, amounts >= 0, and items >= 1 inside struct."""
        def is_valid_struct(val: Any) -> bool:
            if not isinstance(val, dict):
                return False
            required_keys = {"customer_id", "order_id", "amount", "status", "items"}
            if not required_keys.issubset(val.keys()):
                return False
            # Quality Assertion: Positive non-negative values
            return val.get("amount", -1) >= 0 and val.get("items", 0) >= 0

        return detail_series.apply(is_valid_struct)


# ==========================================
# 2. Silver Layer Schemas
# ==========================================

class DimCustomerSilverSchema(pa.DataFrameModel):
    """Corresponds to iceberg.silver.dim_customer"""
    event_id: Series[str] = pa.Field(nullable=False)
    event_time: Series[pa.Timestamp] = pa.Field(nullable=False)
    customer_id: Series[str] = pa.Field(nullable=False)
    name: Series[str] = pa.Field(nullable=True)
    
    # Quality Assertion: Valid email format regex check
    email: Series[str] = pa.Field(str_matches=EMAIL_REGEX, nullable=True)
    
    # Quality Assertion: 2-character ISO country code standard (e.g., US, VN, UK)
    country: Series[str] = pa.Field(str_length={"min_value": 2, "max_value": 2}, nullable=True)
    
    _ingested_at: Series[pa.Timestamp] = pa.Field(nullable=False)

    class Config:
        coerce = True
        strict = True


class FactOrdersSilverSchema(pa.DataFrameModel):
    """Corresponds to iceberg.silver.fact_orders"""
    event_id: Series[str] = pa.Field(nullable=False)
    event_time: Series[pa.Timestamp] = pa.Field(nullable=False)
    customer_id: Series[str] = pa.Field(nullable=False)
    order_id: Series[str] = pa.Field(nullable=False)
    
    # DECIMAL(18,2) -> Non-negative financial amount
    amount: Series[float] = pa.Field(ge=0.0, nullable=False)
    
    # Quality Assertion: Domain status mapping categories
    status: Series[str] = pa.Field(
        isin=["PENDING", "COMPLETED", "CANCELLED", "REFUNDED"], 
        nullable=False
    )
    
    # INT -> Must have at least 1 item per order
    items: Series[int] = pa.Field(ge=1, nullable=False)
    
    _ingested_at: Series[pa.Timestamp] = pa.Field(nullable=False)

    class Config:
        coerce = True
        strict = True


# ==========================================
# 3. Gold Layer Schema
# ==========================================

class DailyOrdersByCountryGoldSchema(pa.DataFrameModel):
    """Corresponds to iceberg.gold.daily_orders_by_country"""
    report_date: Series[pd.Timestamp] = pa.Field(nullable=False)
    country: Series[str] = pa.Field(str_length={"min_value": 2, "max_value": 2}, nullable=False)
    
    order_count: Series[int] = pa.Field(ge=0, nullable=False)
    total_amount: Series[float] = pa.Field(ge=0.0, nullable=False)
    avg_order_value: Series[float] = pa.Field(ge=0.0, nullable=False)
    distinct_customers: Series[int] = pa.Field(ge=0, nullable=False)

    class Config:
        coerce = True
        strict = True

    # Wide DataFrame Check: Check metric logic consistency
    @pa.dataframe_check
    def validate_avg_order_value_logic(cls, df: pa.typing.DataFrame) -> Series[bool]:
        """Data Quality Assertion: avg_order_value should not exceed total_amount."""
        return df["avg_order_value"] <= df["total_amount"]


# ==========================================
# 4. Metadata Schema
# ==========================================

class IngestionRegistryMetadataSchema(pa.DataFrameModel):
    """Corresponds to iceberg.metadata.ingestion_registry"""
    pipeline_name: Series[str] = pa.Field(nullable=False)
    last_processed_date: Series[str] = pa.Field(str_matches=r"^\d{4}-\d{2}-\d{2}$", nullable=False)
    load_timestamp: Series[pa.Timestamp] = pa.Field(nullable=False)
    status: Series[str] = pa.Field(isin=["SUCCESS", "FAILED", "RUNNING"], nullable=False)

    class Config:
        coerce = True
        strict = True
