import sys
import os
from pyspark.sql.functions import input_file_name, current_timestamp, date_format, lit, col

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def ingest_green():
    x, y = sys.argv[1].split('-') # 2026-06
    year = int(x)
    month = int(y)

    month -= 1
    if month <= 0:
        month += 12
        year -= 1
    process_month = f"{year}-{month:02d}" # 2026-05

    spark = getOrCreateSparkSession()

    data_path = os.environ.get("DATA_DIR", "/app/data")
    green_pattern = os.path.join(data_path, "green", f"green_tripdata_{process_month}.parquet")

    import glob #scan files
    files = glob.glob(green_pattern)
    if not files:
        print('[Error]: FILE not FOUND')
        sys.exit(99)
    
    for f in files:
        df = spark.read.parquet(f)
        df_meta = df.withColumn('_source_file', input_file_name()) \
                    .withColumn('_ingested_at', current_timestamp ()) \
                    .withColumn('pickup_month', date_format('lpep_pickup_datetime', 'yyyy-MM'))

        target_columns = [
            "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag", 
            "RatecodeID", "PULocationID", "DOLocationID", "passenger_count", "trip_distance", 
            "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee", 
            "improvement_surcharge", "total_amount", "payment_type", "trip_type", 
            "congestion_surcharge", "_source_file", "_ingested_at", "pickup_month"
        ]

        for col_name in target_columns:
            if col_name not in df_meta.columns:
                df_meta = df_meta.withColumn(col_name, lit(None).cast("double" if col_name == "ehail_fee" else "string"))

        df_final = df_meta.select(*target_columns)
        df_final = df_final.filter(col('pickup_month') == process_month)
        df_final.writeTo("bronze.green_trips").append()
        df_final.writeTo("bronze.green_trips").overwritePartitions() #-> use -> fix incremental processing
if __name__ =='__main__':
    ingest_green()