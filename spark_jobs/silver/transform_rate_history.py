import sys
import os
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType, DateType, BooleanType
from pyspark.sql import Row
import pyspark.sql.functions as F

# Add parent directory to path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def get_month_dates(month_str):
    """Returns (first_day_current, last_day_prev) as string YYYY-MM-DD"""
    first_day = f"{month_str}-01"
    dt = datetime.strptime(first_day, "%Y-%m-%d")
    last_day_prev = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    return first_day, last_day_prev

def transform_rate_history():
    spark = getOrCreateSparkSession()
    
    # 1. Check if fact_trips has data
    try:
        df_fact = spark.table("silver.fact_trips")
        if df_fact.count() == 0:
            print("[Error] fact_trips is empty. Nothing to process.")
            spark.stop()
            return
    except Exception as e:
        print(f"Error reading silver.fact_trips: {e}")
        spark.stop()
        return

    # 2. Get distinct sorted months from fact_trips
    months = [row['pickup_month'] for row in df_fact.select("pickup_month").distinct().orderBy("pickup_month").collect()]
    print(f"Months found in fact_trips: {months}")
    
    # Define schema for rate history
    schema = StructType([
        StructField("zone_id", IntegerType(), True),
        StructField("taxi_type", StringType(), True),
        StructField("avg_fare_per_mile", DoubleType(), True),
        StructField("avg_tip_pct", DoubleType(), True),
        StructField("avg_total_amount", DoubleType(), True),
        StructField("trip_count", LongType(), True),
        StructField("version", IntegerType(), True),
        StructField("effective_from", DateType(), True),
        StructField("effective_to", DateType(), True),
        StructField("is_current", BooleanType(), True)
    ])
    
    # Initialize empty dim_rate_history if it doesn't have records
    try:
        df_history = spark.table("silver.dim_rate_history")
        history_count = df_history.count()
        print(f"Current dim_rate_history row count: {history_count}")
    except Exception:
        # Create empty DataFrame if table not initialized
        df_history = spark.createDataFrame([], schema)
        history_count = 0
    
    # We will process each month sequentially
    for month in months:
        print(f"\n--- Processing Month: {month} ---")
        first_day_curr, last_day_prev = get_month_dates(month)
        first_day_curr_dt = datetime.strptime(first_day_curr, "%Y-%m-%d").date()
        last_day_prev_dt = datetime.strptime(last_day_prev, "%Y-%m-%d").date()
        
        # Check if this month has already been processed in history
        # (i.e. is there any record with effective_from >= first_day_curr)
        if history_count > 0:
            already_processed = df_history.filter(
                (F.col("effective_from") == F.lit(first_day_curr))
            ).count() > 0
            if already_processed:
                print(f"Month {month} has already been processed in history. Skipping.")
                continue
        
        # Calculate monthly metrics for the month
        print(f"Calculating metrics for {month}...")
        df_monthly = df_fact.filter(F.col("pickup_month") == month) \
            .groupBy(F.col("pu_location_id").alias("zone_id"), "taxi_type") \
            .agg(
                F.avg(F.col("fare_amount") / F.col("trip_distance_miles")).alias("avg_fare_per_mile"),
                F.avg(F.col("tip_amount") / F.col("fare_amount") * 100.0).alias("avg_tip_pct"),
                F.avg("total_amount").alias("avg_total_amount"),
                F.count("*").alias("trip_count")
            )
            
        if df_monthly.count() == 0:
            print(f"No records found for month {month}. Skipping.")
            continue
            
        if history_count == 0:
            # First month initial load - all records version 1
            print("Executing initial history load...")
            df_new_records = df_monthly.select(
                "zone_id", "taxi_type", "avg_fare_per_mile", "avg_tip_pct", "avg_total_amount", "trip_count",
                F.lit(1).alias("version"),
                F.lit(first_day_curr_dt).alias("effective_from"),
                F.lit(None).cast(DateType()).alias("effective_to"),
                F.lit(True).alias("is_current")
            )
            df_history = df_new_records
            history_count = df_history.count()
        else:
            # Subsequent month incremental load
            # Separate existing history into current and historical
            df_hist_current = df_history.filter(F.col("is_current") == True)
            df_hist_archived = df_history.filter(F.col("is_current") == False)
            
            # Join current history with new monthly metrics
            joined = df_hist_current.alias("curr").join(
                df_monthly.alias("new"),
                (F.col("curr.zone_id") == F.col("new.zone_id")) & 
                (F.col("curr.taxi_type") == F.col("new.taxi_type")),
                "outer"
            )
            
            # Identify updates/inserts
            # We compare if change in avg_fare_per_mile or avg_tip_pct or avg_total_amount >= 5%
            # Or if it is a brand new zone/taxi_type combination
            change_condition = (
                (F.col("new.avg_fare_per_mile").isNotNull()) & (F.col("curr.avg_fare_per_mile").isNotNull()) &
                (
                    (F.abs(F.col("new.avg_fare_per_mile") - F.col("curr.avg_fare_per_mile")) / F.col("curr.avg_fare_per_mile") >= 0.05) |
                    (F.abs(F.col("new.avg_tip_pct") - F.col("curr.avg_tip_pct")) / F.col("curr.avg_tip_pct") >= 0.05) |
                    (F.abs(F.col("new.avg_total_amount") - F.col("curr.avg_total_amount")) / F.col("curr.avg_total_amount") >= 0.05)
                )
            )
            
            # 1. Records that remain current (no change, diff < 5%)
            no_change = joined.filter(
                (F.col("curr.zone_id").isNotNull()) & 
                ((F.col("new.zone_id").isNull()) | (~change_condition))
            ).select(
                "curr.zone_id", "curr.taxi_type", "curr.avg_fare_per_mile", "curr.avg_tip_pct", 
                "curr.avg_total_amount", "curr.trip_count", "curr.version", "curr.effective_from", 
                "curr.effective_to", "curr.is_current"
            )
            
            # 2. Records that must be expired (change >= 5%)
            expired = joined.filter(
                (F.col("curr.zone_id").isNotNull()) & (F.col("new.zone_id").isNotNull()) & change_condition
            ).select(
                "curr.zone_id", "curr.taxi_type", "curr.avg_fare_per_mile", "curr.avg_tip_pct", 
                "curr.avg_total_amount", "curr.trip_count", "curr.version", "curr.effective_from",
                F.lit(last_day_prev_dt).alias("effective_to"),
                F.lit(False).alias("is_current")
            )
            
            # 3. New versions of changed records + new zones
            new_current = joined.filter(
                (F.col("new.zone_id").isNotNull()) & 
                ((F.col("curr.zone_id").isNull()) | change_condition)
            ).select(
                F.col("new.zone_id"), 
                F.col("new.taxi_type"), 
                F.col("new.avg_fare_per_mile"), 
                F.col("new.avg_tip_pct"), 
                F.col("new.avg_total_amount"), 
                F.col("new.trip_count"),
                F.coalesce(F.col("curr.version") + 1, F.lit(1)).alias("version"),
                F.lit(first_day_curr_dt).alias("effective_from"),
                F.lit(None).cast(DateType()).alias("effective_to"),
                F.lit(True).alias("is_current")
            )
            
            # Combine all records
            df_history = df_hist_archived \
                .unionByName(no_change) \
                .unionByName(expired) \
                .unionByName(new_current)
            
            history_count = df_history.count()
            
    # Write back the final updated history to silver.dim_rate_history
    print("Writing updated rate history back to silver.dim_rate_history...")
    df_history.writeTo("silver.dim_rate_history").createOrReplace()
    
    print("SCD Type 2 rate history processing completed successfully!")
    spark.stop()

if __name__ == "__main__":
    transform_rate_history()
