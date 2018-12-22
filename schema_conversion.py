'''
Conversion utilities to common schema format
'''

import pyspark
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

from schema import *
from pyspark.sql.types import *

def payment_type_string_2_id(payment_type_string):

    if payment_type_string is None:
        return None

    payment_type_string = payment_type_string.lower()

    if payment_type_string == "crd":
        return 1
    elif payment_type_string == "csh":
        return 2
    elif payment_type_string == "unk":
        return 5
    elif payment_type_string == "noc":
        return 3
    elif payment_type_string == "dis":
        return 4
    else:
        return None

def vendor_string_2_id(vendor_string):

    if vendor_string is None:
        return None

    vendor_string = vendor_string.lower()

    if vendor_string == "cmt":
        return 1
    elif vendor_string == "vts":
        return 2
    elif vendor_string == "dds":
        return 3
    else:
        return None

def v1_yellow_to_common(dataset, conversion_udf, vendor_conversion_udf, payment_type_conversion_udf):
    dataset = dataset.select(
        vendor_conversion_udf("VendorID".lower()).alias(vendor_id_property),
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
        payment_type_conversion_udf("Payment_type".lower()).alias(payment_type_property)
    )

    return dataset

def v2_yellow_to_common(dataset, conversion_udf, vendor_conversion_udf, payment_type_conversion_udf):
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

def v3_yellow_to_common(dataset, conversion_udf, vendor_conversion_udf, payment_type_conversion_udf):
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

def v1_green_to_common(dataset, conversion_udf, vendor_conversion_udf, payment_type_conversion_udf):
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

def v2_green_to_common(dataset, conversion_udf, vendor_conversion_udf, payment_type_conversion_udf):
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

def v3_green_to_common(dataset, conversion_udf, vendor_conversion_udf, payment_type_conversion_udf):
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

