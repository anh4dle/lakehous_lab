from pyspark.sql import SparkSession

class SparkClient:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("lakehouse")
            .getOrCreate()
        )

    def read_json(self, path):
        return self.spark.read.json(path)

    def read_iceberg(self, table):
        return self.spark.read.format("iceberg").load(table)

    def write_iceberg(self, df, table, mode="append"):
        (
            df.write
            .format("iceberg")
            .mode(mode)
            .save(table)
        )