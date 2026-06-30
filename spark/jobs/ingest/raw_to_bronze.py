from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col, current_timestamp, to_timestamp
from datetime import datetime
from spark.spark_client import SparkClient
# ========================= CONFIG =========================
BASE_INPUT_PATH = "s3a://warehouse/raw/customers"        # ← Change if bucket name is different
TABLE_FULL_NAME = "iceberg.bronze.fact_customer_events"
CONTROL_TABLE = "iceberg.bronze.load_control"
WAREHOUSE_PATH = "/tmp/iceberg_warehouse"   # Local Iceberg warehouse
# =========================================================

def get_last_processed_date(spark) -> str:
    """Retrieve last successful processed date"""
    df = spark.sql(f"""
        SELECT COALESCE(MAX(processed_date), '1900-01-01') as last_date 
        FROM {CONTROL_TABLE}
    """)
    return df.collect()[0]["last_date"]


def read_new_data(spark, base_path: str, last_processed_date: str):
    """Read all data from a path into a DataFrame"""
    
    # Generate list of dates to read (you can make this smarter)
    from datetime import datetime, timedelta
    import pandas as pd   # or use pure python
    
    #Get dates that are not yet processed
    start_date = datetime.strptime(last_processed_date, "%Y-%m-%d")
    dates = pd.date_range(start=start_date + timedelta(days=1), end=datetime.today()).strftime('%Y-%m-%d').tolist()
    
    if not dates:
        return spark.createDataFrame([], schema="temp_schema")
    
    paths = [f"{base_path}/dt={d}/customers_*.json" for d in dates]
    
    df_raw = (spark.read
              .option("multiLine", "true")
              .option("mode", "PERMISSIVE")
              .json(paths))   
    
    # Add ingestion metadata
    df = (df_raw
          .withColumn("pipeline_name", "customer_raw_to_bronze")
          .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"))
          .withColumn("_ingested_at", current_timestamp()))
    
    return df


def write_to_iceberg(df, table_name: str):
    """Append to Iceberg table"""
    (df.writeTo(table_name)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .option("merge-schema", "true")
        .append())


def update_control_table(spark, max_date: str, record_count: int):
    """Record successful load"""
    control_df = spark.createDataFrame(
        [(max_date, datetime.now(), record_count, "SUCCESS")],
        ["processed_date", "load_timestamp", "records_processed", "status"]
    )
    control_df.writeTo(CONTROL_TABLE).append()

# ========================= MAIN FUNCTION =========================
def main():
    spark = SparkClient()
    
    print(f"Base input path : {BASE_INPUT_PATH}")
    print(f"Target table    : {TABLE_FULL_NAME}")
    
    
    last_date = get_last_processed_date(spark)
    print(f"Last processed date: {last_date}")
    
    df = read_new_data(spark, BASE_INPUT_PATH, last_date)
    
    new_count = df.count()
    print(f"Found {new_count} new records to ingest.")
    
    if new_count > 0:
        write_to_iceberg(df, TABLE_FULL_NAME)
        
        max_date = df.selectExpr("max(dt_folder)").collect()[0][0]
        update_control_table(spark, max_date, new_count)
        
        print(f"Successfully loaded {new_count} records into Iceberg.")
    else:
        print("No new data to process.")
    
    print("=== Incremental load finished ===")


if __name__ == "__main__":
    main()