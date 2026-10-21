from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

SPARK_JARS = (
    "/opt/spark/extra_jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar,"
    "/opt/spark/extra_jars/iceberg-aws-bundle-1.6.1.jar,"
    "/opt/spark/extra_jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark/extra_jars/aws-java-sdk-bundle-1.12.262.jar"
)

spark_common_args = {
    "conn_id": "spark_default",
    "py_files": "/opt/airflow/src.zip",
    "jars": SPARK_JARS,
    "executor_memory": "1g",
    "executor_cores": 1,
    "total_executor_cores": 1,
    "conf": {
        "spark.default.parallelism": "2",
        "spark.sql.shuffle.partitions": "2",
        "spark.executorEnv.MALLOC_ARENA_MAX": "2",
        "spark.executor.extraJavaOptions": "-XX:ErrorFile=/tmp/hs_err_%p.log",
    },
}

with DAG(
    dag_id="backfill_dag",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    # Example using Jinja macro to fallback if dag_run.conf is empty
    raw_to_bronze = SparkSubmitOperator(
        task_id="raw_to_bronze",
        application="/opt/airflow/dags/scripts/bronze_pipeline.py",
        application_args=[
            "--mode",
            "{{ dag_run.conf.get('mode', 'incremental') }}",
            "--start-date",
            "{{ dag_run.conf.get('start_date', '') }}",
            "--end-date",
            "{{ dag_run.conf.get('end_date', '') }}",
        ],
        **spark_common_args,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/airflow/dags/scripts/silver_pipeline.py",
        application_args=[
            "--mode",
            "{{ dag_run.conf.get('mode', 'incremental') }}",
            "--start-date",
            "{{ dag_run.conf.get('start_date', '') }}",
            "--end-date",
            "{{ dag_run.conf.get('end_date', '') }}",
        ],
        **spark_common_args,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/airflow/dags/scripts/gold_pipeline.py",
        application_args=[
            "--mode",
            "{{ dag_run.conf.get('mode', 'incremental') }}",
            "--start-date",
            "{{ dag_run.conf.get('start_date', '') }}",
            "--end-date",
            "{{ dag_run.conf.get('end_date', '') }}",
        ],
        **spark_common_args,
    )

    raw_to_bronze >> bronze_to_silver >> silver_to_gold
