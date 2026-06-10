import sys
import os
from pyspark.sql.functions import col, lit, md5, concat_ws, coalesce, when, date_format, to_date, hour, dayofweek, row_number
from pyspark.sql.window import Window

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

# fact
def transform_trips():
    spark = getOrCreateSparkSession()
    
    x, y = sys.argv[1].split('-') # 2026-06
    if not x and not y: 
        sys.exit(99)
    else:
        year = int(x)
        month = int(y)

        month -= 1
        if month <= 0:
            month += 12
            year -= 1
        process_month = f"{year}-{month:02d}" # 2026-05

    df_green = spark.table('bronze.green_trips')
    df_green = df_green.filter(col('pickup_month') == process_month)
    df_yellow = spark.table('bronze.yellow_trips')
    df_yellow = df_yellow.filter(col('pickup_month') == process_month)

    df_yellow_mapped = df_yellow.select(
        md5(concat_ws('-', 
            lit('yellow'), 
            col('tpep_pickup_datetime').cast('string'), 
            col('tpep_dropoff_datetime').cast('string'), 
            col('PULocationID').cast('string'), 
            col('DOLocationID').cast('string'))).alias('trip_id'),
        lit('yellow').alias('taxi_type'),
        col('VendorID').cast('int').alias('vendor_id'),
        col('tpep_pickup_datetime').alias('pickup_datetime'),
        col('tpep_dropoff_datetime').alias('dropoff_datetime'),
        coalesce(col('passenger_count').cast('int'), lit(1)).alias('passenger_count'),
        col('trip_distance').cast('double').alias('trip_distance_miles'),
        when(col('RatecodeID') == 99, lit(1)).otherwise(col('RatecodeID').cast('int')).alias('rate_code'),
        when(col('store_and_fwd_flag') == 'Y', lit(True)).otherwise(lit(False)).alias('store_and_fwd'),
        col('PULocationID').cast('int').alias('pu_location_id'),
        col('DOLocationID').cast('int').alias('do_location_id'),
        col('payment_type').cast('int').alias('payment_type'),
        col('fare_amount').cast('double').alias('fare_amount'),
        col('extra').cast('double').alias('extra'),
        col('mta_tax').cast('double').alias('mta_tax'),
        col('tip_amount').cast('double').alias('tip_amount'),
        col('tolls_amount').cast('double').alias('tolls_amount'),
        col('improvement_surcharge').cast('double').alias('improvement_surcharge'),
        col('congestion_surcharge').cast('double').alias('congestion_surcharge'),
        col('airport_fee').cast('double').alias('airport_fee'),
        col('total_amount').cast('double').alias('total_amount'),
        lit(None).cast('int').alias('trip_type'),
        col('_ingested_at')
    )

    df_green_mapped = df_green.select(
        md5(concat_ws('-', 
            lit('green'), 
            col('lpep_pickup_datetime').cast('string'), 
            col('lpep_dropoff_datetime').cast('string'), 
            col('PULocationID').cast('string'), 
            col('DOLocationID').cast('string'))).alias('trip_id'),
        lit('green').alias('taxi_type'),
        col('VendorID').cast('int').alias('vendor_id'),
        col('lpep_pickup_datetime').alias('pickup_datetime'),
        col('lpep_dropoff_datetime').alias('dropoff_datetime'),
        coalesce(col('passenger_count').cast('int'), lit(1)).alias('passenger_count'),
        col('trip_distance').cast('double').alias('trip_distance_miles'),
        when(col('RatecodeID') == 99, lit(1)).otherwise(col('RatecodeID').cast('int')).alias('rate_code'),
        when(col('store_and_fwd_flag') == 'Y', lit(True)).otherwise(lit(False)).alias('store_and_fwd'),
        col('PULocationID').cast('int').alias('pu_location_id'),
        col('DOLocationID').cast('int').alias('do_location_id'),
        col('payment_type').cast('int').alias('payment_type'),
        col('fare_amount').cast('double').alias('fare_amount'),
        col('extra').cast('double').alias('extra'),
        col('mta_tax').cast('double').alias('mta_tax'),
        col('tip_amount').cast('double').alias('tip_amount'),
        col('tolls_amount').cast('double').alias('tolls_amount'),
        col('improvement_surcharge').cast('double').alias('improvement_surcharge'),
        col('congestion_surcharge').cast('double').alias('congestion_surcharge'),
        lit(None).cast('double').alias('airport_fee'),
        col('total_amount').cast('double').alias('total_amount'),
        col('trip_type').cast('int').alias('trip_type'),
        col('_ingested_at')
    )

    # union
    df_unified = df_yellow_mapped.unionByName(df_green_mapped)

    # create anything for query
    df_with_duration = df_unified.withColumn(
        "trip_duration_minutes", 
        (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60.0
    )

    df_cleaned = df_with_duration \
        .filter(col("trip_distance_miles") > 0.0) \
        .filter(col("pickup_datetime") < col("dropoff_datetime"))
    
    df_final = df_cleaned \
        .withColumn("trip_distance_km", col("trip_distance_miles") * 1.61) \
        .withColumn("pickup_date", to_date(col("pickup_datetime"))) \
        .withColumn("pickup_hour", hour(col("pickup_datetime"))) \
        .withColumn("pickup_day_of_week", ((dayofweek(col("pickup_datetime")) + 5) % 7) + 1) \
        .withColumn("is_weekend", col("pickup_day_of_week").isin(6, 7)) \
        .withColumn("pickup_month", date_format(col("pickup_datetime"), "yyyy-MM"))
    
    # Deduplicate
    window_spec = Window.partitionBy("trip_id").orderBy(col("_ingested_at").desc())
    df_deduped = df_final \
        .withColumn("_row_num", row_number().over(window_spec)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num") \
        .drop("_ingested_at") # Drop metadata _ingested_at to match schema
    
    df_output = df_deduped.select(
        "trip_id", "taxi_type", "vendor_id", "pickup_datetime", "dropoff_datetime",
        "trip_duration_minutes", "passenger_count", "trip_distance_miles", "trip_distance_km",
        "rate_code", "store_and_fwd", "pu_location_id", "do_location_id", "payment_type",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "congestion_surcharge", "airport_fee", "total_amount",
        "trip_type", "pickup_date", "pickup_hour", "pickup_day_of_week", "is_weekend", "pickup_month"
    )
    
    print(f"Overwriting data in silver.fact_trips for partition pickup_month={process_month}...")
    df_output.writeTo("silver.fact_trips").overwritePartitions()

if __name__ == "__main__":
    transform_trips()
