from pyspark.sql import SparkSession
import os

def getOrCreateSparkSession():
    # env
    CATALOG_URI = os.environ.get("CATALOG_URI", "http://localhost:8181")
    S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
    S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")

    spark = SparkSession.builder.appName('Spark Job') \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.lakehouse.type", "rest") \
            .config("spark.sql.catalog.lakehouse.uri", CATALOG_URI) \
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT) \
            .config("spark.sql.catalog.lakehouse.s3.access-key-id", S3_ACCESS_KEY) \
            .config("spark.sql.catalog.lakehouse.s3.secret-access-key", S3_SECRET_KEY) \
            .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
            .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1") \
            .config("spark.sql.catalog.lakehouse.client.region", "us-east-1") \
            .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse") \
            .config("spark.sql.defaultCatalog", "lakehouse")
    
    return spark.getOrCreate()