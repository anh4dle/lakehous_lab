CREATE TABLE IF NOT EXISTS  iceberg.bronze.fact_customer_events (
    event_id        STRING,
    event_time      TIMESTAMP,
    customer        STRUCT<
        customer_id: STRING,
        name: STRING,
        email: STRING,
        country: STRING,
        created_at: TIMESTAMP
    >,
    _ingested_at    TIMESTAMP,
    _source_file    STRING
)
USING iceberg
PARTITIONED BY (days(event_time));


CREATE TABLE IF NOT EXISTS  iceberg.bronze.fact_orders_events (
    event_id        STRING,
    event_time      TIMESTAMP,
    order_detail    STRUCT<
        customer_id: STRING,
        order_id: STRING,
        amount: FLOAT,
        status: STRING,
        items: INT,
        created_at: TIMESTAMP
    >,
    _ingested_at    TIMESTAMP,
    _source_file    STRING
)
USING iceberg
PARTITIONED BY (days(event_time));

CREATE TABLE IF NOT EXISTS  iceberg.silver.dim_customer (
    customer_id      STRING,
    name             STRING,
    email            STRING,
    country          STRING,
    created_at       TIMESTAMP,
    _valid_from      TIMESTAMP,
    _valid_to        TIMESTAMP,
    _is_current      BOOLEAN,
    _source_event_id STRING
)
USING iceberg
PARTITIONED BY (_is_current);

CREATE TABLE IF NOT EXISTS  iceberg.silver.fact_orders (
    event_id        STRING,
    event_time      TIMESTAMP,
    customer_id     STRING,
    order_id        STRING,
    amount          DECIMAL(18,2),
    status          STRING,
    items           INT,
    created_at      TIMESTAMP,
    _ingested_at    TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time));

CREATE TABLE IF NOT EXISTS  iceberg.gold.daily_orders_by_country (
    report_date         DATE,
    country              STRING,
    order_count          BIGINT,
    total_amount         DECIMAL(18,2),
    avg_order_value      DECIMAL(18,2),
    distinct_customers   BIGINT
)
USING iceberg
PARTITIONED BY (report_date);


DROP TABLE IF EXISTS iceberg.metadata.ingestion_registry;
 CREATE TABLE IF NOT EXISTS iceberg.metadata.ingestion_registry (
    pipeline_name STRING,
    last_processed_date STRING,
    load_timestamp TIMESTAMP,
    status STRING
) USING iceberg


