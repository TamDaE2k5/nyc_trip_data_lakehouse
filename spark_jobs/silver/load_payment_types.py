import sys
import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import getOrCreateSparkSession

def load_payment_types():
    spark = getOrCreateSparkSession()

    schema = StructType([
        StructField("payment_type_id", IntegerType(), False),
        StructField("payment_type_desc", StringType(), False)
    ])

    data = [
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip')
    ]

    df = spark.createDataFrame(data, schema)
    df.writeTo('silver.dim_payment_types').createOrReplace()
    print("Successfully loaded silver.dim_payment_types")

if __name__ == '__main__':
    load_payment_types()
