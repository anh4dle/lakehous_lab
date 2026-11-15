from datetime import datetime, timezone, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from feast import FeatureStore

# Configure logging for Airflow task execution tracking
logger = logging.getLogger(__name__)


def run_materialization() -> None:
    """
    Executes Feast materialization
    """
    # Initialize the Feast feature store.
    # Ensure repo_path points to your feature repository directory containing feature_store.yaml.
    store = FeatureStore(repo_path="/opt/airflow/dags/feature_repo")

    # Define the upper bound (high-water mark) for the materialization window
    end_date = datetime.now(timezone.utc)

    logger.info(
        f"Starting Feast batch materialization up to timestamp: {end_date.isoformat()}"
    )

    try:
        # materialize_incremental() checks metadata to compute only the delta
        # since the last successful sync, avoiding redundant data scans over MinIO/Iceberg.
        store.materialize_incremental(end_date=end_date)

        logger.info("Feast incremental materialization completed successfully.")

    except Exception as e:
        logger.error(f"Critical error during Feast batch materialization: {e}")
        # Raising the exception ensures Airflow correctly triggers your retry policy
        raise


# Define standard Airflow default arguments
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    dag_id="feast_daily_orders_materialization",
    default_args=default_args,
    description="Sync Feast batch features from Iceberg to the online store",
    schedule_interval="0 * * * *",  # Run hourly
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    materialize_task = PythonOperator(
        task_id="run_feast_materialization",
        python_callable=run_materialization,
    )

    materialize_task
