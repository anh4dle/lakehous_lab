from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    raw_to_bronze = SparkSubmitOperator(
        task_id="raw_to_bronze",
        conn_id="spark_default",
        application="/opt/airflow/dags/scripts/bronze_pipeline.py",
        py_files="/opt/airflow/src.zip",
        jars=(
        "/opt/spark/extra_jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar,"
        "/opt/spark/extra_jars/iceberg-aws-bundle-1.6.1.jar,"
        "/opt/spark/extra_jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark/extra_jars/aws-java-sdk-bundle-1.12.262.jar"
        ),
        executor_memory="2g",
        driver_memory="2g",
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="spark_default",
        application="/opt/airflow/dags/scripts/silver_pipeline.py",
        py_files="/opt/airflow/src.zip",
        jars=(
        "/opt/spark/extra_jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar,"
        "/opt/spark/extra_jars/iceberg-aws-bundle-1.6.1.jar,"
        "/opt/spark/extra_jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark/extra_jars/aws-java-sdk-bundle-1.12.262.jar"
        ),
        executor_memory="2g",
        driver_memory="2g",
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        conn_id="spark_default",
        application="/opt/airflow/dags/scripts/gold_pipeline.py",
        py_files="/opt/airflow/src.zip",
        jars=(
        "/opt/spark/extra_jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar,"
        "/opt/spark/extra_jars/iceberg-aws-bundle-1.6.1.jar,"
        "/opt/spark/extra_jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark/extra_jars/aws-java-sdk-bundle-1.12.262.jar"
        ),
        executor_memory="2g",
        driver_memory="2g",
    )

    raw_to_bronze >> bronze_to_silver >> silver_to_gold