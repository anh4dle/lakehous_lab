# tests/conftest.py
import pytest
import os
from src.client.spark_client import SparkClient
from src.client.minio_client import MinioClient  

@pytest.fixture(scope="session")
def minio_client():
    """
    Spins up the MinioClient wrapper for the test session.
    Ensures test buckets are created.
    """
    os.environ.setdefault("RUN_MODE", "local")
    client = MinioClient(env="local")
    
    # Ensure default warehouse or test buckets exist before tests run
    client.create_bucket_if_not_exists("warehouse")
    
    return client

@pytest.fixture(scope="session")
def spark_client():
    """
    Spins up the SparkClient wrapper once per test session.
    """
    os.environ.setdefault("RUN_MODE", "local")
    client = SparkClient(env="local")
    
    yield client
    
    if client.spark:
        client.spark.stop()
        
        
@pytest.fixture(scope="session", autouse=True)
def setup_test_namespaces(spark_client):
    """Ensure test namespaces exist in the Iceberg catalog before tests run."""
    spark_client.spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.bronze_test;")
    yield