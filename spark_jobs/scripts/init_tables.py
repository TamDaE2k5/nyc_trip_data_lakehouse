import sys
import os

# import utils for spark sesssion
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import getOrCreateSparkSession

def init_tables_lakehouse():

    spark = getOrCreateSparkSession()

    # 1. create namespace. for postgre -> create metadata, for minio -> create path: warehouse/bronze
    spark.sql('CREATE NAMESPACE IF NOT EXISTS bronze')
    spark.sql('CREATE NAMESPACE IF NOT EXISTS silver')
    spark.sql('CREATE NAMESPACE IF NOT EXISTS gold')

    #2. create tables
    spark.sql(
        '''CREATE TABLE IF NOT EXISTS bronze.green_trips (
        VendorID                INT,
        lpep_pickup_datetime    TIMESTAMP,
        lpep_dropoff_datetime   TIMESTAMP,
        store_and_fwd_flag      STRING,
        RatecodeID              INT,
        PULocationID            INT,
        DOLocationID            INT,
        passenger_count         INT,
        trip_distance           DOUBLE,
        fare_amount             DOUBLE,
        extra                   DOUBLE,
        mta_tax                 DOUBLE,
        tip_amount              DOUBLE,
        tolls_amount            DOUBLE,
        ehail_fee               DOUBLE,
        improvement_surcharge   DOUBLE,
        total_amount            DOUBLE,
        payment_type            INT,
        trip_type               INT,
        congestion_surcharge    DOUBLE,
        _source_file            STRING,
        _ingested_at            TIMESTAMP,
        pickup_month            STRING
        ) USING iceberg
        PARTITIONED BY (pickup_month) '''
    )

    spark.sql(
        '''CREATE TABLE IF NOT EXISTS bronze.yellow_trips (
        VendorID                INT,
        tpep_pickup_datetime    TIMESTAMP,
        tpep_dropoff_datetime   TIMESTAMP,
        passenger_count         INT,
        trip_distance           DOUBLE,
        RatecodeID              INT,
        store_and_fwd_flag      STRING,
        PULocationID            INT,
        DOLocationID            INT,
        payment_type            INT,
        fare_amount             DOUBLE,
        extra                   DOUBLE,
        mta_tax                 DOUBLE,
        tip_amount              DOUBLE,
        tolls_amount            DOUBLE,
        improvement_surcharge   DOUBLE,
        total_amount            DOUBLE,
        congestion_surcharge    DOUBLE,
        airport_fee             DOUBLE,
        _source_file            STRING,
        _ingested_at            TIMESTAMP,
        pickup_month            STRING
        ) USING iceberg
        PARTITIONED BY (pickup_month) '''
    )

    spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.fact_trips (
        trip_id                 STRING,
        taxi_type               STRING,
        vendor_id               INT,
        pickup_datetime         TIMESTAMP,
        dropoff_datetime        TIMESTAMP,
        trip_duration_minutes   DOUBLE, 
        passenger_count         INT,
        trip_distance_miles     DOUBLE,
        trip_distance_km        DOUBLE,
        rate_code               INT,
        store_and_fwd           BOOLEAN,
        pu_location_id          INT,
        do_location_id          INT,
        payment_type            INT,
        fare_amount             DOUBLE,
        extra                   DOUBLE,
        mta_tax                 DOUBLE,
        tip_amount              DOUBLE,
        tolls_amount            DOUBLE,
        improvement_surcharge   DOUBLE,
        congestion_surcharge    DOUBLE,
        airport_fee             DOUBLE,
        total_amount            DOUBLE,
        trip_type               INT,
        pickup_date             DATE,
        pickup_hour             INT,
        pickup_day_of_week      INT,
        is_weekend              BOOLEAN,
        pickup_month            STRING
    ) USING iceberg
    PARTITIONED BY (pickup_month)
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.dim_zones (
        location_id             INT,
        borough                 STRING,
        zone                    STRING,
        service_zone            STRING
    ) USING iceberg
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.dim_rate_history (
        zone_id                 INT,
        taxi_type               STRING,
        avg_fare_per_mile       DOUBLE,
        avg_tip_pct             DOUBLE,
        avg_total_amount        DOUBLE,
        trip_count              BIGINT,
        version                 INT,
        effective_from          DATE,
        effective_to            DATE,
        is_current              BOOLEAN
    ) USING iceberg
    """)

    # Create and seed silver.dim_payment_types
    spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.dim_payment_types (
        payment_type_id         INT,
        payment_type_desc       STRING
    ) USING iceberg
    """)

    if spark.table("silver.dim_payment_types").count() == 0:
        spark.sql("""
        INSERT INTO silver.dim_payment_types VALUES
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip')
        """)

    # Create and seed silver.dim_rate_codes
    spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.dim_rate_codes (
        rate_code_id            INT,
        rate_code_desc          STRING
    ) USING iceberg
    """)

    if spark.table("silver.dim_rate_codes").count() == 0:
        spark.sql("""
        INSERT INTO silver.dim_rate_codes VALUES
        (1, 'Standard rate'),
        (2, 'JFK'),
        (3, 'Newark'),
        (4, 'Nassau or Westchester'),
        (5, 'Negotiated fare'),
        (6, 'Group ride')
        """)


if __name__ == '__main__':
    init_tables_lakehouse()