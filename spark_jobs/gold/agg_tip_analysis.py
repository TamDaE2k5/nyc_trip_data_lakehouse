import sys
import os
from pyspark.sql import functions as F

# Add parent directory to path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def agg_tip_analysis():
    x,y = sys.argv[1].split('-')
    year = int(x)
    month = int(y)
    month-=1
    if month<=0:
        month+=12
        year-=1
    process_month = f'{year}-{month:02d}'
    
    print(f"Starting Gold tip analysis aggregation in incremental mode for month: {process_month}...")
        
    spark = getOrCreateSparkSession()
    
    # 1. Read Silver tables
    df_fact = spark.table("silver.fact_trips")
    df_zones = spark.table("silver.dim_zones")
    df_payment_types = spark.table("silver.dim_payment_types")
    
    if process_month:
        print(f"Filtering silver.fact_trips for pickup_month = '{process_month}'...")
        df_fact = df_fact.filter(F.col("pickup_month") == process_month)
    
    # 2. Join fact, zones, and payment types
    df_joined = df_fact \
        .join(df_zones, df_fact.pu_location_id == df_zones.location_id, "inner") \
        .join(df_payment_types, df_fact.payment_type == df_payment_types.payment_type_id, "inner")
    
    # 3. Add airport trip flag
    # rate_code: 2 = JFK, 3 = Newark
    df_with_airport = df_joined.withColumn(
        "is_airport_trip",
        (F.col("rate_code").isin(2, 3)) | 
        (F.col("zone").isin("JFK Airport", "LaGuardia Airport", "Newark Airport"))
    )
    
    # 4. Aggregate metrics by group
    print("Aggregating tip metrics...")
    df_metrics = df_with_airport.groupBy(
        F.col("pickup_month").alias("analysis_month"),
        "taxi_type",
        "payment_type_desc",
        "borough",
        "is_airport_trip"
    ).agg(
        F.count("*").alias("total_trips"),
        F.sum(F.when(F.col("tip_amount") > 0, 1).otherwise(0)).alias("trips_with_tip"),
        F.avg("tip_amount").alias("avg_tip_amount"),
        F.avg(F.col("tip_amount") / F.col("fare_amount") * 100.0).alias("avg_tip_pct"),
        F.max("tip_amount").alias("max_tip")
    )
    
    # 5. Compute tip_rate_pct
    df_final = df_metrics.withColumn(
        "tip_rate_pct",
        (F.col("trips_with_tip") / F.col("total_trips")) * 100.0
    )
    
    # Rearrange columns to match table schema exactly
    df_output = df_final.select(
        "analysis_month", "taxi_type", "payment_type_desc", "borough",
        "total_trips", "trips_with_tip", "tip_rate_pct", "avg_tip_amount", "avg_tip_pct", "max_tip",
        "is_airport_trip"
    )
    
    # 6. Write to gold.dm_tip_analysis
    print("Writing to gold.dm_tip_analysis...")
    print(f"Overwriting data in gold.dm_tip_analysis for partition analysis_month={process_month}...")
    df_output.writeTo("gold.dm_tip_analysis").overwritePartitions()

if __name__ == "__main__":
    agg_tip_analysis()
