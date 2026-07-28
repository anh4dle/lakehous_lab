
def validate_schema(spark_client, df, table_name):
    """Check if the schema of the DataFrame matches
    the target Iceberg table schema
    iceberg.bronze.fact_customer_events
    """
    target_schema = spark_client.spark.table(table_name).schema
    print("Showing target schema:", target_schema)
    print("Showing source schema:")
    df.printSchema()
    for source_field, target_field in zip(df.schema, target_schema):
        assert source_field.name == target_field.name
        assert source_field.dataType == target_field.dataType