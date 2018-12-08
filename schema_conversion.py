'''
Conversion utilities to common schema format
'''

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

from original_schemas import *
from schema import *

from pyspark.sql.types import *
v3_schema_yellow = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("Passenger_count", IntegerType(), True),
        StructField("Trip_distance", DoubleType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("Store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("Payment_type", IntegerType(), True),
        StructField("Fare_amount", DoubleType(), True),
        StructField("Extra", DoubleType(), True),
        StructField("MTA_tax", DoubleType(), True),
        StructField("Tip_amount", DoubleType(), True),
        StructField("Tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("Total_amount", DoubleType(), True),
    ]
)

def v3_yellow_to_common(dataset):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("yellow").alias(taxi_company_property),
        col("lpep_pickup_datetime".lower()).alias(pickup_datetime_property),
        col("lpep_dropoff_datetime".lower()).alias(dropoff_datetime_property),
        col("PULocationID".lower()).alias(pickup_location_id_property),
        col("DOLocationID".lower()).alias(dropoff_location_id_property),
        col("Passenger_count".lower()).alias(passenger_count_property),
        col("Trip_distance".lower()).alias(trip_distance_property),
        col("Store_and_fwd_flag".lower()).alias(store_and_forward_flag_property),
        col("RateCodeID".lower()).alias(ratecode_id_property),
        col("Fare_amount".lower()).alias(fare_amount_property),
        col("Tolls_amount".lower()).alias(tolls_amount_property),
        col("Total_amount".lower()).alias(total_amount_property),
        col("MTA_tax".lower()).alias(mta_tax_property),
        col("improvement_surcharge".lower()).alias(improvement_surcharge_property),
        col("Extra".lower()).alias(extra_property),
        col("Tip_amount".lower()).alias(tip_amount_property),
        col("Payment_type".lower()).alias(payment_type_property)
    )

    return dataset