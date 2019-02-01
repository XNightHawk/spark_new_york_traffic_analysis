'''

  ________    ______   ______     _          ____        __
 /_  __/ /   / ____/  /_  __/____(_)___     / __ \____ _/ /_____ _
  / / / /   / /        / / / ___/ / __ \   / / / / __ `/ __/ __ `/
 / / / /___/ /___     / / / /  / / /_/ /  / /_/ / /_/ / /_/ /_/ /
/_/ /_____/\____/    /_/ /_/  /_/ .___/  /_____/\__,_/\__/\__,_/
                               /_/


Authors: Willi Menapace <willi.menapace@studenti.unitn.it>
         Luca Zanells <luca.zanella-3@studenti.unitn.it>

Schemas of original data

Some fields may not have the same name as in the original data archives in order to achieve better uniformity
'''

import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.types import *

#Yellow taxi schema 2013-2014
v1_schema_yellow = StructType(
    [
        StructField("VendorID", StringType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("Passenger_count", IntegerType(), True),
        StructField("Trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("Store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("Payment_type", StringType(), True),
        StructField("Fare_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("MTA_tax", DoubleType(), True),
        StructField("Tip_amount", DoubleType(), True),
        StructField("Tolls_amount", DoubleType(), True),
        StructField("Total_amount", DoubleType(), True),
    ]
)

#Yellow taxi schema 2015-2016
v2_schema_yellow = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("Passenger_count", IntegerType(), True),
        StructField("Trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("Store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
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

#Yellow taxi schema 2017-2018
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

#Green taxi schema 2013-2014
v1_schema_green = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("Store_and_fwd_flag", StringType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("Pickup_longitude", DoubleType(), True),
        StructField("Pickup_latitude", DoubleType(), True),
        StructField("Dropoff_longitude", DoubleType(), True),
        StructField("Dropoff_latitude", DoubleType(), True),
        StructField("Passenger_count", IntegerType(), True),
        StructField("Trip_distance", DoubleType(), True),
        StructField("Fare_amount", DoubleType(), True),
        StructField("Extra", DoubleType(), True),
        StructField("MTA_tax", DoubleType(), True),
        StructField("Tip_amount", DoubleType(), True),
        StructField("Tolls_amount", DoubleType(), True),
        StructField("Ehail_fee", DoubleType(), True),
        StructField("Total_amount", DoubleType(), True),
        StructField("Payment_type", IntegerType(), True),
        StructField("Trip_type", IntegerType(), True)
    ]
)

#Green taxi schema 2015-2016
v2_schema_green = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("Store_and_fwd_flag", StringType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("Pickup_longitude", DoubleType(), True),
        StructField("Pickup_latitude", DoubleType(), True),
        StructField("Dropoff_longitude", DoubleType(), True),
        StructField("Dropoff_latitude", DoubleType(), True),
        StructField("Passenger_count", IntegerType(), True),
        StructField("Trip_distance", DoubleType(), True),
        StructField("Fare_amount", DoubleType(), True),
        StructField("Extra", DoubleType(), True),
        StructField("MTA_tax", DoubleType(), True),
        StructField("Tip_amount", DoubleType(), True),
        StructField("Tolls_amount", DoubleType(), True),
        StructField("Ehail_fee", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("Total_amount", DoubleType(), True),
        StructField("Payment_type", IntegerType(), True),
        StructField("Trip_type", IntegerType(), True)
    ]
)

#Green taxi schema 2017-2018
v3_schema_green = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("Store_and_fwd_flag", StringType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("Passenger_count", IntegerType(), True),
        StructField("Trip_distance", DoubleType(), True),
        StructField("Fare_amount", DoubleType(), True),
        StructField("Extra", DoubleType(), True),
        StructField("MTA_tax", DoubleType(), True),
        StructField("Tip_amount", DoubleType(), True),
        StructField("Tolls_amount", DoubleType(), True),
        StructField("Ehail_fee", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("Total_amount", DoubleType(), True),
        StructField("Payment_type", IntegerType(), True),
        StructField("Trip_type", IntegerType(), True)
    ]
)