import time
START_TS_MS = int(time.time() * 1000)
from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment
from src.core.config import (JAR_DIR, FLINK_REST_HOST, 
                             FLINK_REST_PORT, KAFKA_BOOTSTRAP, PG_URL, PG_USER, PG_PASSWORD, JAR_FILES)


def build_table_env() -> TableEnvironment:
    """TableEnvironment is the client object to use Flink Table API and SQL"""    
    config = Configuration()
    config.set_string("execution.target", "remote")
    config.set_string("rest.address", FLINK_REST_HOST)
    config.set_string("rest.port", FLINK_REST_PORT)
    config.set_string("parallelism.default", "1")
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("jobmanager.rpc.address", "flink-jobmanager")
    config.set_string("table.local-time-zone", "UTC")
    settings = (
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .with_configuration(config)
        .build()
    )
    t_env = TableEnvironment.create(settings)

    # Local JARs: used by the client for planning and uploaded to the cluster on submit
    jar_paths = [JAR_DIR / name for name in JAR_FILES]
    missing = [str(p) for p in jar_paths if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing connector JARs:\n  " + "\n  ".join(missing))

    t_env.get_config().set("pipeline.jars", ";".join(p.as_uri() for p in jar_paths))
    return t_env

def create_tables(t_env: TableEnvironment) -> None:
    """
    Create source and sink tables
    """
    t_env.execute_sql(f"""
        CREATE TABLE sku_inventory_source (
            sku_id STRING,
            product_name STRING,
            warehouse_code STRING,
            available_qty INT,
            reorder_threshold INT,
            updated_at BIGINT,
            updated_ts AS CAST(TO_TIMESTAMP_LTZ(updated_at / 1000, 3) AS TIMESTAMP(3)),
            PRIMARY KEY (sku_id) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'pg_server.public.sku_inventory',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-sku-badge-processor',
            'scan.startup.mode' = 'timestamp',
            'scan.startup.timestamp-millis' = '{START_TS_MS}',
            'format' = 'debezium-json',
            'debezium-json.schema-include' = 'true'
        )
    """)

    # Postgres JDBC sink (upsert on sku_id)
    t_env.execute_sql(f"""
        CREATE TABLE live_stock_status (
            sku_id STRING,
            product_name STRING,
            warehouse_code STRING,
            available_qty INT,
            stock_badge STRING,
            last_updated TIMESTAMP(3),
            PRIMARY KEY (sku_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'live_stock_status',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}'
        )
    """)


def run_pipeline() -> None:
    """
    Create the tables, processing logic and submit the job
    """
    t_env = build_table_env()
    create_tables(t_env)

    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql("""
        INSERT INTO live_stock_status
        SELECT
            sku_id,
            product_name,
            warehouse_code,
            available_qty,
            CASE
                WHEN available_qty <= 0 THEN 'OUT_OF_STOCK'
                WHEN available_qty <= reorder_threshold THEN 'LOW_STOCK'
                ELSE 'IN_STOCK'
            END AS stock_badge,
            updated_ts AS last_updated
        FROM sku_inventory_source
    """)

    print(f"Submitting Flink streaming job to {FLINK_REST_HOST}:{FLINK_REST_PORT} ...")
    result = statement_set.execute()
    job_client = result.get_job_client()
    job_id = job_client.get_job_id() if job_client else "unknown"
    print(f"Job submitted (id: {job_id}). UI: http://{FLINK_REST_HOST}:{FLINK_REST_PORT}")

if __name__ == "__main__":
    run_pipeline()