-- ============================================================
-- BRONZE – add new nested fields
-- ============================================================
ALTER TABLE iceberg.bronze.fact_customer_events 
ADD COLUMN customer.device_id STRING;

ALTER TABLE iceberg.bronze.fact_customer_events 
ADD COLUMN customer.tier STRING;


ALTER TABLE iceberg.bronze.fact_orders_events 
ADD COLUMN `order`.session_id STRING;

ALTER TABLE iceberg.bronze.fact_orders_events 
ADD COLUMN `order`.promo_code STRING;


-- ============================================================
-- SILVER – add the corresponding flat columns
-- ============================================================
ALTER TABLE iceberg.silver.dim_customer 
ADD COLUMN device_id STRING;

ALTER TABLE iceberg.silver.dim_customer 
ADD COLUMN tier STRING;


ALTER TABLE iceberg.silver.fact_orders 
ADD COLUMN session_id STRING;

ALTER TABLE iceberg.silver.fact_orders 
ADD COLUMN promo_code STRING;