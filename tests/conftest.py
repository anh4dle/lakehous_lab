# tests/conftest.py
import pytest
import os
from src.client.spark_client import SparkClient


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