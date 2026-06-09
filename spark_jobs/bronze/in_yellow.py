import sys
import os
import glob
from pyspark.sql.functions import lit, current_timestamp, input_file_name, date_format, col

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def ingest_yellow():
    x, y = sys.argv[1].split('-') # 2026-06
    year = int(x)
    month = int(y)

    month -= 3
    if month <= 0:
        month += 12
        year -= 1
    process_month = f"{year}-{month:02d}" # 2026-05

    spark = getOrCreateSparkSession()

    data_path = os.environ.get("DATA_DIR", "/app/data")
    yellow_pattern = os.path.join(data_path, "yellow", f"yellow_tripdata_{process_month}.parquet")

    files = glob.glob(yellow_pattern)
    if not files:
        print('[ERROR] File not found')
        sys.exit(99)

    for f in files:
        df = spark.read.parquet(f)
        df_meta = df.withColumn('_source_file', input_file_name()) \
                    .withColumn("_ingested_at", current_timestamp()) \
                    .withColumn("pickup_month", date_format("tpep_pickup_datetime", "yyyy-MM"))
        
        target_columns = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", 
            "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", 
            "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", 
            "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee", 
            "_source_file", "_ingested_at", "pickup_month"
        ]
        
        for col_name in target_columns:
            if col_name not in df_meta.columns:
                df_meta = df_meta.withColumn(col_name, lit(None).cast("double" if col_name == "airport_fee" else "string"))
                
        df_final = df_meta.select(*target_columns)
        df_final = df_final.filter(col('pickup_month') == process_month)
        # df_final.writeTo("bronze.yellow_trips").append()
        df_final.writeTo("bronze.yellow_trips").overwritePartitions() #-> use -> fix incremental processing
    

if __name__=='__main__':
    ingest_yellow()