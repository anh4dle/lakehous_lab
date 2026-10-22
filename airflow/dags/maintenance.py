from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='lakehouse_iceberg_maintenance',
    default_args=default_args,
    description='Automated maintenance DAG for Apache Iceberg tables via Spark Submit',
    schedule='@weekly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['lakehouse', 'iceberg', 'maintenance', 'spark'],
) as dag:

    target_tables = [
        "iceberg.bronze.fact_customer_events",
        # "iceberg.silver.dim_customers",
    ]

    for table in target_tables:
        table_slug = table.replace(".", "_")

        # Step 1: Bin-Pack Compaction
        compact_task = SparkSubmitOperator(
            task_id=f'compact_{table_slug}',
            application='/opt/airflow/src/pipelines/iceberg_maintenance.py',
            conn_id='spark_default',  # Uses your AIRFLOW_CONN_SPARK_DEFAULT environment connection
            conf={
                'spark.master': 'spark://spark:7077',
                'spark.submit.pyFiles': '/opt/airflow/src.zip', # Optional: if you package your src folder
            },
            packages=(
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                "org.apache.iceberg:iceberg-aws-bundle:1.6.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4"
            ),
            application_args=[
                '--action', 'compact',
                '--table', table
            ],
            env_vars={'RUN_MODE': 'docker'},
        )

        # Step 2: Expire Old Snapshots
        expire_snapshots_task = SparkSubmitOperator(
            task_id=f'expire_snapshots_{table_slug}',
            application='/opt/airflow/src/pipelines/iceberg_maintenance.py',
            conn_id='spark_default',
            conf={'spark.master': 'spark://spark:7077'},
            packages=(
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                "org.apache.iceberg:iceberg-aws-bundle:1.6.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4"
            ),
            application_args=[
                '--action', 'expire_snapshots',
                '--table', table,
                '--retention-days', '7'
            ],
            env_vars={'RUN_MODE': 'docker'},
        )

        # Step 3: Remove Orphan Files
        remove_orphans_task = SparkSubmitOperator(
            task_id=f'remove_orphans_{table_slug}',
            application='/opt/airflow/src/pipelines/iceberg_maintenance.py',
            conn_id='spark_default',
            conf={'spark.master': 'spark://spark:7077'},
            packages=(
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                "org.apache.iceberg:iceberg-aws-bundle:1.6.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4"
            ),
            application_args=[
                '--action', 'remove_orphans',
                '--table', table,
                '--older-than-days', '3'
            ],
            env_vars={'RUN_MODE': 'docker'},
        )

        # Enforce sequential lifecycle dependency chain
        compact_task >> expire_snapshots_task >> remove_orphans_task