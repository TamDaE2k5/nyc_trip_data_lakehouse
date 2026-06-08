import sys
import os
from pyspark.sql.functions import col

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def load_zones():
    spark = getOrCreateSparkSession()

    data = os.path.join('/app/data', 'lookup', 'taxi_zone_lookup.csv')

    if not os.path.exists(data):
        print(f"[Error] file lookup not found at {data}")
        sys.exit(99)
    
    df = spark.read.option("header", "true").csv(data)
    df_final = df.select(
        col('LocationID').cast('int').alias('location_id'),
        col('Borough').alias('borough'),
        col('Zone').alias('zone'),
        col('service_zone')
    )

    df_final.writeTo('silver.dim_zones').createOrReplace()

if __name__ =='__main__':
    load_zones()