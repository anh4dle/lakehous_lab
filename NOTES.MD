-- ============================================================
-- BRONZE – add new nested fields
-- ============================================================
ALTER TABLE iceberg.bronze.fact_customer_events 
ADD COLUMN customer.device_id VARCHAR;

ALTER TABLE iceberg.bronze.fact_customer_events 
ADD COLUMN customer.tier VARCHAR;


ALTER TABLE iceberg.bronze.fact_orders_events 
ADD COLUMN `order`.session_id VARCHAR;

ALTER TABLE iceberg.bronze.fact_orders_events 
ADD COLUMN `order`.promo_code VARCHAR;


-- ============================================================
-- SILVER – add the corresponding flat columns
-- ============================================================
ALTER TABLE iceberg.silver.dim_customer 
ADD COLUMN device_id VARCHAR;

ALTER TABLE iceberg.silver.dim_customer 
ADD COLUMN tier VARCHAR;


ALTER TABLE iceberg.silver.fact_orders 
ADD COLUMN session_id VARCHAR;

ALTER TABLE iceberg.silver.fact_orders 
ADD COLUMN promo_code VARCHAR;


-- ============================================================
-- GOLD
-- ============================================================

ALTER TABLE gold.daily_orders_by_country
ADD COLUMN _ingested_at TIMESTAMP;

# Read message from topic
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic pg_server.public.sku_inventory \
    --from-beginning \
    --max-messages 1

# Postgres connector for Debezium
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "airflow",
    "database.password": "airflow",
    "database.dbname": "airflow",
    "topic.prefix": "pg_server",
    "table.include.list": "public.sku_inventory"
  }
}