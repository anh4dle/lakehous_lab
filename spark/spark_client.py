from pyspark.sql import SparkSession
from pathlib import Path
import pyspark


class SparkClient:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("lakehouse_lab")
            # Iceberg
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # REST catalog
            .config(
                "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.iceberg.type", "rest")
            .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181")
            # warehouse
            .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse")
            # S3
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .getOrCreate()
        )

    def read_json(self, path):
        return self.spark.read.json(path)

    def read_iceberg(self, table):
        return self.spark.read.format("iceberg").load(table)

    def write_iceberg(self, df, table, mode="append"):
        (df.write.format("iceberg").mode(mode).save(table))

    def bootstrap_sql_tables(self):
        base_dir = Path(__file__).parent.parent
        sql_dir = base_dir / "sql" / "all_ddl.sql"
        with open(sql_dir) as f:
            sql = f.read()
            for statement in sql.split(";"):
                if statement.strip():
                    self.spark.sql(statement)
