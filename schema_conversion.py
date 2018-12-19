'''
Conversion utilities to common schema format
'''

import os
import random

import pyspark
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
import numpy as np
import shapely.geometry
import fiona
import pyproj
from schema import *
from pyspark.sql.types import *

import configuration

def v1_yellow_to_common(dataset, conversion_udf):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("yellow").alias(taxi_company_property),
        col("lpep_pickup_datetime".lower()).alias(pickup_datetime_property),
        col("lpep_dropoff_datetime".lower()).alias(dropoff_datetime_property),
        conversion_udf("pickup_latitude", "pickup_longitude").alias(pickup_location_id_property),
        conversion_udf("dropoff_latitude", "dropoff_longitude").alias(dropoff_location_id_property),
        col("Passenger_count".lower()).alias(passenger_count_property),
        col("Trip_distance".lower()).alias(trip_distance_property),
        col("Store_and_fwd_flag".lower()).alias(store_and_forward_flag_property),
        col("RateCodeID".lower()).alias(ratecode_id_property),
        col("Fare_amount".lower()).alias(fare_amount_property),
        col("Tolls_amount".lower()).alias(tolls_amount_property),
        col("Total_amount".lower()).alias(total_amount_property),
        col("MTA_tax".lower()).alias(mta_tax_property),
        col("improvement_surcharge".lower()).alias(improvement_surcharge_property),
        lit(0).alias(extra_property),
        col("Tip_amount".lower()).alias(tip_amount_property),
        col("Payment_type".lower()).alias(payment_type_property)
    )

    return dataset

def v2_yellow_to_common(dataset, conversion_udf):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("yellow").alias(taxi_company_property),
        col("lpep_pickup_datetime".lower()).alias(pickup_datetime_property),
        col("lpep_dropoff_datetime".lower()).alias(dropoff_datetime_property),
        conversion_udf("pickup_latitude", "pickup_longitude").alias(pickup_location_id_property),
        conversion_udf("dropoff_latitude", "dropoff_longitude").alias(dropoff_location_id_property),
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

def v3_yellow_to_common(dataset, conversion_udf):
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

def v1_green_to_common(dataset, conversion_udf):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("green").alias(taxi_company_property),
        col("lpep_pickup_datetime".lower()).alias(pickup_datetime_property),
        col("lpep_dropoff_datetime".lower()).alias(dropoff_datetime_property),
        conversion_udf("Pickup_latitude", "Pickup_longitude").alias(pickup_location_id_property),
        conversion_udf("Dropoff_latitude", "Dropoff_longitude").alias(dropoff_location_id_property),
        col("Passenger_count".lower()).alias(passenger_count_property),
        col("Trip_distance".lower()).alias(trip_distance_property),
        col("Store_and_fwd_flag".lower()).alias(store_and_forward_flag_property),
        col("RateCodeID".lower()).alias(ratecode_id_property),
        col("Fare_amount".lower()).alias(fare_amount_property),
        col("Tolls_amount".lower()).alias(tolls_amount_property),
        col("Total_amount".lower()).alias(total_amount_property),
        col("MTA_tax".lower()).alias(mta_tax_property),
        lit(0).alias(improvement_surcharge_property),
        col("Extra".lower()).alias(extra_property),
        col("Tip_amount".lower()).alias(tip_amount_property),
        col("Payment_type".lower()).alias(payment_type_property)
    )

    return dataset

def v2_green_to_common(dataset, conversion_udf):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("green").alias(taxi_company_property),
        col("lpep_pickup_datetime".lower()).alias(pickup_datetime_property),
        col("lpep_dropoff_datetime".lower()).alias(dropoff_datetime_property),
        conversion_udf("Pickup_latitude", "Pickup_longitude").alias(pickup_location_id_property),
        conversion_udf("Dropoff_latitude", "Dropoff_longitude").alias(dropoff_location_id_property),
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

def v3_green_to_common(dataset, conversion_udf):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("green").alias(taxi_company_property),
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

