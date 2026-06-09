import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Add parent directory to path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def agg_revenue_by_zone():
    x,y = sys.argv[1].split('-')
    year = int(x)
    month = int(y)
    month-=3
    if month<=0:
        month+=12
        year-=1
    process_month = f'{year}-{month:02d}'
    print(f"Starting Gold revenue by zone aggregation in incremental mode for month: {process_month}...")
        
    spark = getOrCreateSparkSession()
    
    # 1. Read Silver tables
    df_fact = spark.table("silver.fact_trips")
    df_zones = spark.table("silver.dim_zones")
    
    if process_month:
        print(f"Filtering silver.fact_trips for pickup_month = '{process_month}'...")
        df_fact = df_fact.filter(F.col("pickup_month") == process_month)
    
    # 2. Join fact and zones
    df_joined = df_fact.join(df_zones, df_fact.pu_location_id == df_zones.location_id, "inner")
    
    # 3. Aggregate metrics by month, borough, zone, taxi_type
    print("Aggregating revenue metrics...")
    df_metrics = df_joined.groupBy(
        F.col("pickup_month").alias("analysis_month"),
        "borough", "zone", "taxi_type"
    ).agg(
        F.count("*").alias("total_trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.sum("tip_amount").alias("total_tips"),
        F.avg("total_amount").alias("avg_revenue_per_trip")
    )
    
    # 4. Rank zones by revenue within each (month, borough)
    window_spec = Window.partitionBy("analysis_month", "borough").orderBy(F.col("total_revenue").desc())
    
    df_ranked = df_metrics.withColumn("revenue_rank", F.rank().over(window_spec))
    
    # Rearrange columns to match table schema exactly
    df_output = df_ranked.select(
        "analysis_month", "borough", "zone", "taxi_type",
        "total_trips", "total_revenue", "total_tips", "avg_revenue_per_trip",
        "revenue_rank"
    )
    
    # 5. Write to gold.dm_revenue_by_zone
    print("Writing to gold.dm_revenue_by_zone...")
    print(f"Overwriting data in gold.dm_revenue_by_zone for partition analysis_month={process_month}...")
    df_output.writeTo("gold.dm_revenue_by_zone").overwritePartitions()

if __name__ == "__main__":
    agg_revenue_by_zone()
