import sys
import os
from datetime import datetime

# Add parent directory to path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def cleanup_snapshots():
    spark = getOrCreateSparkSession()
    
    tables = [
        "bronze.green_trips",
        "bronze.yellow_trips",
        "silver.fact_trips",
        "silver.dim_zones",
        "silver.dim_rate_history",
        "silver.dim_payment_types",
        "silver.dim_rate_codes",
        "gold.dm_revenue_by_zone",
        "gold.dm_hourly_demand",
        "gold.dm_tip_analysis"
    ]
    
    # Get current timestamp for expire_snapshots
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    for table in tables:
        try:
            print(f"\n=== Cleaning up table: {table} ===")
            
            # 1. Expire old snapshots (keep at least the last 1 snapshot)
            print(f"Expiring snapshots older than {current_time}...")
            spark.sql(f"""
                CALL lakehouse.system.expire_snapshots(
                    table => '{table}',
                    older_than => TIMESTAMP '{current_time}',
                    retain_last => 1
                )
            """).show(truncate=False)
            
            # 2. Remove orphan files
            print(f"Removing orphan files...")
            spark.sql(f"""
                CALL lakehouse.system.remove_orphan_files(
                    table => '{table}'
                )
            """).show(truncate=False)
            
        except Exception as e:
            print(f"[Warning] Failed to clean up table {table}: {e}")

    print("\nCleanup completed successfully!")
    spark.stop()

if __name__ == "__main__":
    cleanup_snapshots()
