from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

# Iceberg maintenance through Trino (ALTER TABLE ... EXECUTE).
# Requires the apache-airflow-providers-trino package and an Airflow
# connection named "trino_default" that points at the Trino coordinator.

TRINO_CONN_ID = "trino_default"

# Trino enforces a minimum retention on expire_snapshots and remove_orphan_files
# (7d by default). Going lower needs a change to the catalog's min-retention
# properties in Trino, so keep these at 7d unless you have changed that.
EXPIRE_RETENTION = "7d"
ORPHAN_RETENTION = "7d"

# Files smaller than this are candidates for compaction.
OPTIMIZE_FILE_SIZE_THRESHOLD = "128MB"

TARGET_TABLES = [
    "iceberg.bronze.fact_customer_events",
    # "iceberg.ops.quarantine_dlq",
    # "iceberg.silver.dim_customers",
    # "iceberg.gold.<your_gold_table>",
    # "iceberg.ops.<your_registry_table>",
]

default_args = {
    "owner": "lakehouse_admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_iceberg_maintenance_v2",
    default_args=default_args,
    description="Weekly Iceberg maintenance (compact, expire snapshots, remove orphans) via Trino",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Never overlap two maintenance runs, and keep to one task at a time so the
    # Trino cluster is not hit by several heavy statements at once.
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["lakehouse", "iceberg", "maintenance", "trino"],
) as dag:

    for table in TARGET_TABLES:
        slug = table.replace(".", "_")

        # One group per table: the three steps are strictly ordered, but a failure
        # on one table does not block the chains of the other tables.
        with TaskGroup(group_id=slug) as table_group:

            # Step 1: bin-pack small files into larger ones.
            compact = SQLExecuteQueryOperator(
                task_id="compact",
                conn_id=TRINO_CONN_ID,
                autocommit=True,
                sql=(
                    f"ALTER TABLE {table} EXECUTE optimize"
                    f"(file_size_threshold => '{OPTIMIZE_FILE_SIZE_THRESHOLD}')"
                ),
            )

            # Step 2: drop old snapshots so replaced files are no longer referenced.
            expire_snapshots = SQLExecuteQueryOperator(
                task_id="expire_snapshots",
                conn_id=TRINO_CONN_ID,
                autocommit=True,
                sql=(
                    f"ALTER TABLE {table} EXECUTE expire_snapshots"
                    f"(retention_threshold => '{EXPIRE_RETENTION}')"
                ),
            )

            # Step 3: delete files that no snapshot or metadata file references.
            # Keep the retention longer than your longest-running write job.
            remove_orphans = SQLExecuteQueryOperator(
                task_id="remove_orphan_files",
                conn_id=TRINO_CONN_ID,
                autocommit=True,
                sql=(
                    f"ALTER TABLE {table} EXECUTE remove_orphan_files"
                    f"(retention_threshold => '{ORPHAN_RETENTION}')"
                ),
            )

            compact >> expire_snapshots >> remove_orphans