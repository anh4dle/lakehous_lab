from datetime import timedelta

from feast import Entity, FeatureView, Field, PushSource
from feast.types import Float64, Int64, String
from feast.value_type import ValueType
from feast.infra.data_sources.contrib.iceberg_catalog import IcebergSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

country = Entity(name="country", join_keys=["country"], value_type=ValueType.STRING)
sku = Entity(name="sku", join_keys=["sku_id"], value_type=ValueType.INT64)

# 1. Batch source: Iceberg gold table via the tabulario REST catalog
daily_orders_source = IcebergSource(
    name="daily_orders_by_country",
    warehouse="",
    catalog_name="my_catalog",
    namespace="gold",
    table="daily_orders_by_country",
    catalog_type="rest",
    endpoint="http://iceberg-rest:8181",
    catalog_properties={
        "s3.endpoint": "http://minio:9000",
        "s3.path-style-access": "true",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
    },
    timestamp_field="_ingested_at",
)

daily_orders_fv = FeatureView(
    name="daily_country_orders",
    entities=[country],
    ttl=timedelta(days=7),
    schema=[
        Field(name="order_count", dtype=Int64),
        Field(name="total_amount", dtype=Float64),
        Field(name="avg_order_value", dtype=Float64),
        Field(name="distinct_customers", dtype=Int64),
    ],
    online=True,
    source=daily_orders_source,
)

# 2. Streaming source: Flink -> Postgres + Feast push API
live_stock_pg_source = PostgreSQLSource(
    name="live_stock_status_pg",
    query="SELECT sku_id, warehouse_code, available_qty, stock_badge, last_updated FROM live_stock_status",
    timestamp_field="last_updated",
)

live_stock_push_source = PushSource(
    name="live_stock_push_source",
    batch_source=live_stock_pg_source,
)

live_stock_fv = FeatureView(
    name="live_sku_stock",
    entities=[sku],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="warehouse_code", dtype=String),
        Field(name="available_qty", dtype=Int64),
        Field(name="stock_badge", dtype=String),
    ],
    online=True,
    source=live_stock_push_source,
)