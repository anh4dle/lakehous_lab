from pyspark.sql import SparkSession
from pathlib import Path


class SparkClient:
    def __init__(self):
        packages = [
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
            "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
            "org.apache.hadoop:hadoop-aws:3.4.1",  # match your Spark's bundled Hadoop version
        ]

        self.spark = (
            SparkSession.builder.appName("lakehouse_lab")
            .config("spark.jars.packages", ",".join(packages))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # Iceberg REST catalog (unchanged, uses S3FileIO for table data)
            .config(
                "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.iceberg.type", "rest")
            .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181")
            .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse")
            .config(
                "spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config("spark.sql.catalog.iceberg.s3.endpoint", "http://localhost:9000")
            .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin")
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin")
            .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
            .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
            # Hadoop S3A config — needed for plain spark.read/write against s3:// paths
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
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
