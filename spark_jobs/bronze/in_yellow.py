import sys
import os
import glob
from pyspark.sql.functions import lit, current_timestamp, input_file_name, date_format

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def ingest_yellow():
    spark = getOrCreateSparkSession()

    data_path = os.environ.get("DATA_DIR", "/app/data")
    yellow_pattern = os.path.join(data_path, "yellow", "yellow_tripdata_*.parquet")

    files = glob.glob(yellow_pattern)
    if not files:
        local_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
        yellow_pattern = os.path.join(local_path, 'yellow', 'yellow_tripdata_*.parquet')
        files = glob.glob(yellow_pattern)
    if not files:
        print('[ERROR] File not found')
        sys.exit(1)

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
        df_final.writeTo("bronze.yellow_trips").append()
    

if __name__=='__main__':
    ingest_yellow()