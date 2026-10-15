## Lakehouse Lab:
- A modern, containerized data lakehouse built with Apache Spark, Apache Iceberg, MinIO (S3-compatible storage), Apache Airflow, and Pandera data contracts. This repository serves as a template and Proof-of-Concept (POC) for data engineering pipelines following the Medallion architecture pattern (Bronze, Silver, Gold).

## Architecture Overview
- The platform uses a decoupled storage and compute model managed via an Iceberg REST catalog and executed through isolated Apache Spark tasks orchestrated by Airflow.

## Medallion Layers:
- Bronze Layer: Ingests raw JSON payloads from MinIO partitioned by date (dt=YYYY-MM-DD).
- Silver Layer: Flattens structs, cleans data types, enforces business-level domain rules via Pandera contracts, and performs idempotent upserts.
- Gold Layer: Aggregate businessm metrics (e.g., daily metrics by country, active customer counts) optimized for analytical consumption.

## Key Technical Features
- Incremental loading: Tracks processing progress via a custom ingestion_registry table, ensuring pipelines only process new daily partitions.
- Schema Validation: Uses Pandera to validate schema fields, data types, nullability, and business ranges.
- Dead Letter Queue (DLQ): Intercepts validation failures and routes malformed records to an iceberg.ops.quarantine_dlq table with full audit metadata (_error_reason, _quarantine_timestamp) without crashing the pipeline.
- Idempotent Upserts: Implements Iceberg MERGE INTO operations to guarantee safe retries and prevent data duplication.
- Containerized Infrastructure: Fully orchestrated via Docker Compose, including Airflow, Spark Master/Worker nodes, MinIO storage, and an Iceberg REST.

## Project Structure

lakehouse_lab/
├── airflow/
│   ├── config/
│   │   └── airflow.cfg
│   ├── dags/
│   │   └── pipeline.py        # Lakehouse pipeline DAG
│   └── Dockerfile
├── data/                      # Local MinIO data
├── data_generator/
│   ├── generate_data.py       # Synthetic customer and order data
│   └── output/                # Generated JSON partitions
├── jars/                      # Spark, Iceberg, and AWS runtime JARs
├── spark/
│   └── Dockerfile
├── sql/
│   └── all_ddl.sql            # Lakehouse table definitions
├── src/
│   ├── client/                # Spark and MinIO clients
│   ├── core/                  # Configuration and logging
│   ├── pipelines/             # Bronze, Silver, and Gold pipelines
│   ├── utils/                 # Registry, I/O, transformations, validation
│   └── main.py                # Application entrypoint
├── Docker-compose.yaml        # Local infrastructure definition
├── Makefile
├── pyproject.toml
└── uv.lock
## Getting Started:
- Prerequisites: 
    - Docker and Docker Compose installed.
    - Python 3.10+ (for local script execution if running outside containers).

0. zip src folder when changes code: zip src code to provide the dependency for Spark
Bash:
rm src.zip
find src -type d -name "__pycache__" -exec rm -r {} +
zip -r src.zip src/

1. Environment Setup
Spin up the containerized stack:
Bash:
docker compose up --build -d
2. Generate Sample Data
Run the synthetic data generator to simulate multi-day transaction streams:
Bash:
python -m src.main
3. Trigger Pipelines via Airflow
Access the Airflow UI at http://localhost:8080 (default credentials: airflow/airflow), enable the lakehouse_pipeline DAG, and trigger a manual run to execute the end-to-end Medallion transition.


