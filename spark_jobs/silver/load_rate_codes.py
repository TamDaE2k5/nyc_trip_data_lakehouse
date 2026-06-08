import sys
import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def load_rate_codes():
    spark = getOrCreateSparkSession()

    schema = StructType([
        StructField("rate_code_id", IntegerType(), False),
        StructField("rate_code_desc", StringType(), False)
    ])

    data = [
        (1, 'Standard rate'),
        (2, 'JFK'),
        (3, 'Newark'),
        (4, 'Nassau or Westchester'),
        (5, 'Negotiated fare'),
        (6, 'Group ride')
    ]

    df = spark.createDataFrame(data, schema)
    df.writeTo('silver.dim_rate_codes').createOrReplace()
    print("Successfully loaded silver.dim_rate_codes")

if __name__ == '__main__':
    load_rate_codes()
