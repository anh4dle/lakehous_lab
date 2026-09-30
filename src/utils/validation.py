def validate_schema(spark_client, source_df, target_table_name):
    """Check if the read schema matches
    the target Iceberg table schema
    iceberg.bronze.fact_customer_events
    """
    target_schema = spark_client.spark.table(target_table_name).schema
    print("Showing target schema:", target_schema)
    print("Showing source schema:")
    source_df.printSchema()
    for source_field, target_field in zip(source_df.schema, target_schema):
        assert source_field.name == target_field.name
        assert source_field.dataType == target_field.dataType
