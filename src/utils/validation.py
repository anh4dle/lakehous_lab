from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType
from src.client.spark_client import SparkClient


def validate_schema(
    spark_client: SparkClient,
    source_df: DataFrame,
    target_table_name: str,
) -> None:
    """
    Validate that the source DataFrame schema matches the target Iceberg table schema.
    """
    print("Showing source schema:")
    source_df.printSchema()

    target_df: DataFrame = spark_client.spark.table(target_table_name)
    target_schema: StructType = target_df.schema
    print("Showing target schema:", target_schema)

    source_schema: StructType = source_df.schema

    # 1. Guard check: Ensure field counts match prior to zipping
    if len(source_schema.fields) != len(target_schema.fields):
        raise ValueError(
            f"Schema length mismatch: source has {len(source_schema.fields)} fields, "
            f"target '{target_table_name}' has {len(target_schema.fields)} fields."
        )

    # 2. Field-by-field position and type validation
    source_field: StructField
    target_field: StructField
    for source_field, target_field in zip(source_schema, target_schema):
        assert source_field.name == target_field.name, (
            f"Field name mismatch: expected '{target_field.name}', "
            f"got '{source_field.name}'"
        )
        assert source_field.dataType == target_field.dataType, (
            f"DataType mismatch for field '{source_field.name}': "
            f"expected {target_field.dataType}, got {source_field.dataType}"
        )