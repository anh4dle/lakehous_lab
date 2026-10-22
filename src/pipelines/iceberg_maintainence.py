import argparse
from pyspark.sql import SparkSession
from src.client.spark_client import SparkClient
from datetime import datetime, timedelta
def main():
    #Test scripts:
    #python -m src.pipelines.iceberg_maintainence --action compact --table iceberg.bronze.fact_customer_events
    #python -m src.pipelines.iceberg_maintainence --action expire_snapshots --table iceberg.bronze.fact_customer_events
    #python -m src.pipelines.iceberg_maintainence --action remove_orphans --table iceberg.bronze.fact_customer_events
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', required=True, choices=['compact', 'expire_snapshots', 'remove_orphans'])
    parser.add_argument('--table', required=True)
    parser.add_argument('--retention-days', type=int, default=777)
    parser.add_argument('--older-than-days', type=int, default=777)
    args = parser.parse_args()

    spark = SparkClient().spark

    # Split the table argument: e.g., 'iceberg.bronze.fact_customer_events'
    # catalog = 'iceberg', table_name = 'bronze.fact_customer_events'
    parts = args.table.split('.')
    if len(parts) >= 3:
        catalog_name = parts[0]
        table_identifier = ".".join(parts[1:])
    else:
        # Fallback if catalog wasn't explicitly prefixed
        catalog_name = "iceberg"
        table_identifier = args.table

    if args.action == 'compact':
        print(f"Running bin-pack compaction on {args.table}...")
        spark.sql(f"""
    CALL {catalog_name}.system.rewrite_data_files(
        table => '{table_identifier}',
        options => map('min-input-files', '2', 'rewrite-all', 'true')
    )
""")

    elif args.action == 'expire_snapshots':
        # Calculate the cutoff timestamp in Python
        cutoff_date = datetime.now() - timedelta(days=args.retention_days)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Expiring snapshots older than {args.retention_days} days ({cutoff_str}) on {args.table}...")
        
        spark.sql(f"""
            CALL {catalog_name}.system.expire_snapshots(
                table => '{table_identifier}', 
                older_than => TIMESTAMP '{cutoff_str}'
            )
        """)

    elif args.action == 'remove_orphans':
        # Calculate the cutoff timestamp for orphan files
        cutoff_date = datetime.now() - timedelta(days=args.older_than_days)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Removing orphan files older than {args.older_than_days} days ({cutoff_str}) on {args.table}...")
        
        spark.sql(f"""
            CALL {catalog_name}.system.remove_orphan_files(
                table => '{table_identifier}', 
                older_than => TIMESTAMP '{cutoff_str}'
            )
        """)
if __name__ == "__main__":
    main()