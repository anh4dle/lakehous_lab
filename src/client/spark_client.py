import os
from pyspark.sql import SparkSession
from src.core.config import ROOT_DIR, RUN_MODE
from src.core.logger import get_logger

logger = get_logger(__name__)


class SparkClient:
    def __init__(self, env=RUN_MODE):
        # Determine environment: 'local' (default) or 'docker'
        # Define endpoints based on the environment
        if env == "docker":
            s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
            catalog_uri = os.getenv("CATALOG_URI", "http://iceberg-rest:8181")
        else:  # local
            s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")
            catalog_uri = os.getenv("CATALOG_URI", "http://localhost:8181")

        # Coordinates targeting Spark 3.5 and Scala 2.12
        packages = [
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
            "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        ]

        self.spark = (
            SparkSession.builder.appName("lakehouse_lab")
            .config("spark.jars.packages", ",".join(packages))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.iceberg.type", "rest")
            .config("spark.sql.catalog.iceberg.uri", catalog_uri)
            .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse")
            .config(
                "spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config("spark.sql.catalog.iceberg.s3.endpoint", s3_endpoint)
            .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin")
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin")
            .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
            .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("spark.hadoop.fs.s3a.region", "us-east-1")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.codegen.wholeStage", "false")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.AnnotationBasedFS")
            .getOrCreate()
        )

    def read_json(self, path):
        return self.spark.read.json(path)

    def read_iceberg(self, table):
        return self.spark.read.format("iceberg").load(table)

    def write_iceberg(self, df, table, mode="append"):
        (df.write.format("iceberg").mode(mode).save(table))

    def bootstrap_sql_tables(self):
        sql_dir = ROOT_DIR / "sql" / "all_ddl.sql"
        with open(sql_dir) as f:
            sql = f.read()
            for statement in sql.split(";"):
                if statement.strip():
                    self.spark.sql(statement)
                    logger.info("SQL executed: %s", statement)
