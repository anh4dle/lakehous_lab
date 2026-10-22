import argparse
from datetime import datetime, timedelta
from src.client.spark_client import SparkClient

#Test scripts:
#python -m src.pipelines.iceberg_maintenance --action compact --table iceberg.bronze.fact_customer_events
#python -m src.pipelines.iceberg_maintenance --action expire_snapshots --table iceberg.bronze.fact_customer_events
#python -m src.pipelines.iceberg_maintenance --action remove_orphans --table iceberg.bronze.fact_customer_events
    
def main():
    parser = argparse.ArgumentParser(description="Apache Iceberg Table Maintenance Utility")
    parser.add_argument('--action', required=True, choices=['compact', 'expire_snapshots', 'remove_orphans'])
    parser.add_argument('--table', required=True, help="Fully qualified table name, e.g., iceberg.bronze.fact_customer_events")
    
    # Reasonable production defaults for data lifecycle policies
    parser.add_argument('--retention-days', type=int, default=7, help="Snapshot retention period for time-travel (default: 7 days)")
    parser.add_argument('--older-than-days', type=int, default=3, help="Age threshold for purging unreferenced orphan files (default: 3 days)")
    
    args = parser.parse_args()

    spark = SparkClient().spark

    # Parse catalog and table identifier
    parts = args.table.split('.')
    if len(parts) >= 3:
        catalog_name = parts[0]
        table_identifier = ".".join(parts[1:])
    else:
        catalog_name = "iceberg"
        table_identifier = args.table

    if args.action == 'compact':
        print(f"Running bin-pack compaction on {args.table}...")
        try: 
            spark.sql(f"""
                CALL {catalog_name}.system.rewrite_data_files(
                    table => '{table_identifier}'
                )
            """)
        except Exception as e:
            print(f"Failed to run compaction on {args.table}: {e}")
            raise

    elif args.action == 'expire_snapshots':
        cutoff_date = datetime.now() - timedelta(days=args.retention_days)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Expiring snapshots older than {args.retention_days} days ({cutoff_str}) on {args.table}...")
        try:
            spark.sql(f"""
                CALL {catalog_name}.system.expire_snapshots(
                    table => '{table_identifier}', 
                    older_than => TIMESTAMP '{cutoff_str}'
                )
            """)
        except Exception as e:
            print(f"Failed to run expire_snapshots on {args.table}: {e}")
            raise

    elif args.action == 'remove_orphans':
        cutoff_date = datetime.now() - timedelta(days=args.older_than_days)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Removing orphan files older than {args.older_than_days} days ({cutoff_str}) on {args.table}...")
        try:
            spark.sql(f"""
                CALL {catalog_name}.system.remove_orphan_files(
                    table => '{table_identifier}', 
                    older_than => TIMESTAMP '{cutoff_str}'
                )
            """)
        except Exception as e:
            print(f"Failed to run remove_orphans on {args.table}: {e}")
            raise

if __name__ == "__main__":
    main()