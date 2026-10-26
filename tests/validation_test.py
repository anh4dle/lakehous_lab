from datetime import datetime, date
from decimal import Decimal
from pyspark.sql import Row
from src.utils.validation import validate_and_split_dataframe


def test_fact_customer_events_validation(spark_client):
    """Test validation and splitting for Bronze customer events."""
    data = [
        # Valid row
        Row(
            event_id="cust_evt_001",
            event_time=datetime(2026, 5, 1, 10, 0, 0),
            _ingested_at=datetime(2026, 5, 1, 10, 5, 0)
        ),
        # Invalid row (missing event_id)
        Row(
            event_id=None,
            event_time=datetime(2026, 5, 1, 10, 10, 0),
            _ingested_at=datetime(2026, 5, 1, 10, 15, 0)
        ),
        # Invalid row (missing event_time)
        Row(
            event_id="cust_evt_002",
            event_time=None,
            _ingested_at=datetime(2026, 5, 1, 10, 20, 0)
        ),
    ]
    df = spark_client.spark.createDataFrame(data)
    validated_df, quarantine_df = validate_and_split_dataframe(
        df, "iceberg.bronze.fact_customer_events"
    )

    assert validated_df.count() == 1
    assert quarantine_df.count() == 2


def test_fact_orders_events_validation(spark_client):
    """Test validation and splitting for Bronze order events."""
    data = [
        # Valid row
        Row(
            event_id="ord_evt_001",
            event_time=datetime(2026, 5, 1, 11, 0, 0),
            _ingested_at=datetime(2026, 5, 1, 11, 5, 0)
        ),
        # Invalid row (missing event_id)
        Row(
            event_id=None,
            event_time=datetime(2026, 5, 1, 11, 10, 0),
            _ingested_at=datetime(2026, 5, 1, 11, 15, 0)
        ),
    ]
    df = spark_client.spark.createDataFrame(data)
    validated_df, quarantine_df = validate_and_split_dataframe(
        df, "iceberg.bronze.fact_orders_events"
    )

    assert validated_df.count() == 1
    assert quarantine_df.count() == 1


def test_dim_customer_validation(spark_client):
    """Test validation for Silver dim_customer."""
    data = [
        # Valid row
        Row(
            event_id="e1",
            event_time=datetime(2026, 5, 1, 12, 0, 0),
            customer_id="c100",
            name="Alice",
            email="alice@test.com",
            country="US",
            _ingested_at=datetime(2026, 5, 1, 12, 5, 0)
        ),
    ]
    df = spark_client.spark.createDataFrame(data)
    validated_df, quarantine_df = validate_and_split_dataframe(
        df, "iceberg.silver.dim_customer"
    )

    assert validated_df.count() == 1
    assert quarantine_df.count() == 0


def test_fact_orders_validation(spark_client):
    """Test validation and splitting for Silver fact_orders with value constraints (amount >= 0, items >= 0)."""
    data = [
        # Valid row
        Row(
            event_id="e1",
            event_time=datetime(2026, 5, 1, 12, 0, 0),
            customer_id="c100",
            order_id="ord_01",
            amount=Decimal("150.50"),
            status="completed",
            items=2,
            _ingested_at=datetime(2026, 5, 1, 12, 5, 0)
        ),
        # Invalid row (null customer_id violates rule)
        Row(
            event_id="e2",
            event_time=datetime(2026, 5, 1, 12, 0, 0),
            customer_id=None,
            order_id="ord_02",
            amount=Decimal("50.00"),
            status="completed",
            items=1,
            _ingested_at=datetime(2026, 5, 1, 12, 5, 0)
        ),
        # Invalid row (negative amount violates rule)
        Row(
            event_id="e3",
            event_time=datetime(2026, 5, 1, 12, 0, 0),
            customer_id="c101",
            order_id="ord_03",
            amount=Decimal("-10.00"),
            status="failed",
            items=1,
            _ingested_at=datetime(2026, 5, 1, 12, 5, 0)
        ),
    ]
    df = spark_client.spark.createDataFrame(data)
    validated_df, quarantine_df = validate_and_split_dataframe(
        df, "iceberg.silver.fact_orders"
    )

    assert validated_df.count() == 1
    assert quarantine_df.count() == 2


def test_daily_orders_by_country_validation(spark_client):
    """Test validation and splitting for Gold daily_orders_by_country aggregated metrics."""
    data = [
        # Valid row
        Row(
            report_date=date(2026, 5, 1),
            country="US",
            order_count=150,
            total_amount=Decimal("12500.00"),
            avg_order_value=Decimal("83.33"),
            distinct_customers=120
        ),
        # Invalid row (negative total_amount violates rule)
        Row(
            report_date=date(2026, 5, 1),
            country="JP",
            order_count=10,
            total_amount=Decimal("-500.00"),
            avg_order_value=Decimal("-50.00"),
            distinct_customers=8
        ),
    ]
    df = spark_client.spark.createDataFrame(data)
    validated_df, quarantine_df = validate_and_split_dataframe(
        df, "iceberg.gold.daily_orders_by_country"
    )

    assert validated_df.count() == 1
    assert quarantine_df.count() == 1