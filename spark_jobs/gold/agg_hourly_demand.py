import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Add parent directory to path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def agg_hourly_demand():

    x,y = sys.argv[1].split('-')
    if not x or not y:
        sys.exit(99)
    
    year = int(x)
    month = int(y)
    month-=1
    if month <= 0:
        month += 12
        year -= 1
    process_month = f'{year}-{month:02d}'

    spark = getOrCreateSparkSession()

    # 1. Read Silver tables
    df_fact = spark.table("silver.fact_trips")
    df_zones = spark.table("silver.dim_zones")
    
    if process_month:
        print(f"Filtering silver.fact_trips for pickup_month = '{process_month}'...")
        df_fact = df_fact.filter(F.col("pickup_month") == process_month)
    
    # 2. Join fact and zones
    df_joined = df_fact.join(df_zones, df_fact.pu_location_id == df_zones.location_id, "inner")
    
    # 3. Calculate peak zone within each group (pickup_date, pickup_hour, taxi_type, borough)
    # 3a. Count trips by specific zone
    df_zone_counts = df_joined.groupBy(
        "pickup_date", "pickup_hour", "taxi_type", "borough", "zone"
    ).agg(
        F.count("*").alias("zone_trips")
    )
    
    # 3b. Use window function to rank zones
    window_spec = Window.partitionBy("pickup_date", "pickup_hour", "taxi_type", "borough") \
                        .orderBy(F.col("zone_trips").desc())
                        
    df_peak_zones = df_zone_counts \
        .withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .select(
            "pickup_date", "pickup_hour", "taxi_type", "borough",
            F.col("zone").alias("peak_zone"),
            F.col("zone_trips").alias("peak_zone_trips")
        )
        
    # 4. Aggregate general metrics by group
    print("Aggregating hourly demand metrics...")
    df_metrics = df_joined.groupBy(
        F.col("pickup_date").alias("analysis_date"), 
        "pickup_hour", "taxi_type", "borough"
    ).agg(
        F.count("*").alias("total_trips"),
        F.avg("trip_distance_km").alias("avg_trip_distance_km"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_min"),
        F.avg("fare_amount").alias("avg_fare")
    )
    
    # 5. Join metrics with peak zone info
    df_output = df_metrics.join(
        df_peak_zones,
        (df_metrics.analysis_date == df_peak_zones.pickup_date) &
        (df_metrics.pickup_hour == df_peak_zones.pickup_hour) &
        (df_metrics.taxi_type == df_peak_zones.taxi_type) &
        (df_metrics.borough == df_peak_zones.borough),
        "inner"
    ).select(
        df_metrics.analysis_date,
        df_metrics.pickup_hour,
        df_metrics.taxi_type,
        df_metrics.borough,
        F.col("total_trips"),
        F.col("avg_trip_distance_km"),
        F.col("avg_trip_duration_min"),
        F.col("avg_fare"),
        F.col("peak_zone"),
        F.col("peak_zone_trips"),
        F.date_format(df_metrics.analysis_date, "yyyy-MM").alias("pickup_month")
    )
    
    # 6. Write (overwrite) to gold.dm_hourly_demand
    print("Writing to gold.dm_hourly_demand...")
    print(f"Overwriting data in gold.dm_hourly_demand for partition pickup_month={process_month}...")
    df_output.writeTo("gold.dm_hourly_demand").overwritePartitions()

if __name__ == "__main__":
    agg_hourly_demand()
