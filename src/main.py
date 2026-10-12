from data_generator.generate_data import run_simulation
from src.client.minio_client import MinioClient
from src.client.spark_client import SparkClient


def generate_data_to_minio():
    run_simulation(7)
    minio_client = MinioClient()
    minio_client.upload_directory("./data_generator/output", "warehouse", "raw")


def main():
    # generate_data_to_minio()
    # spark_client = SparkClient()
    # spark_client.bootstrap_sql_tables()
    pass

main()
