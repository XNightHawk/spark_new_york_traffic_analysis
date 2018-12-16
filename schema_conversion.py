'''
Conversion utilities to common schema format
'''

import pyspark
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
import numpy as np
import shapely.geometry
import fiona
import pyproj
from schema import *
from pyspark.sql.types import *
from configuration import spark

# As the shapefile format is a collection of tree files
# we need to specify the base filename of the shapefile
# or the complete filename of any of the shapefile
# component files
filename = 'docs/taxi_zones/taxi_zones.shp'

# Open a file for reading
taxi_zones_shapes = fiona.open(filename)

num_tiles = 300

# Get bounding coords
minx_global, miny_global, maxx_global, maxy_global = taxi_zones_shapes.bounds
minx_global_bc = spark.sparkContext.broadcast(minx_global)
miny_global_bc = spark.sparkContext.broadcast(miny_global)
maxx_global_bc = spark.sparkContext.broadcast(maxx_global)
maxy_global_bc = spark.sparkContext.broadcast(maxy_global)

# Width of each tile
dx = (maxx_global_bc.value - minx_global_bc.value) / num_tiles
dx_bc = spark.sparkContext.broadcast(dx)
# Height of each tile
dy = (maxy_global_bc.value - miny_global_bc.value) / num_tiles
dy_bc = spark.sparkContext.broadcast(dy)

# Create a num_tiles * num_tiles matrix of None elements
# Alternative: nonelist = [None] * num_tiles * num_tiles
# matrix = np.array(nonelist)
# matrix = np.reshape(matrix, (num_tiles, num_tiles))
matrix = np.full((num_tiles, num_tiles), None)
matrix_bc = spark.sparkContext.broadcast(matrix)

# Populate matrix
for shapefile_record in taxi_zones_shapes:
    # Use Shapely to create the polygon
    shape = shapely.geometry.asShape(shapefile_record['geometry'])
    minx, miny, maxx, maxy = shape.bounds
    minx_index = int((minx - minx_global_bc.value) / dx_bc.value)
    maxx_index = int((maxx - minx_global_bc.value) / dx_bc.value)
    if (maxx == maxx_global_bc.value):
        maxx_index -= 1
    miny_index = int((miny - miny_global_bc.value) / dy_bc.value)
    maxy_index = int((maxy - miny_global_bc.value) / dy_bc.value)
    if (maxy == maxy_global_bc.value):
        maxy_index -= 1

    for i in range(minx_index, maxx_index + 1):
        for j in range(miny_index, maxy_index + 1):
            if(matrix_bc.value[i][j] is None):
                matrix_bc.value[i][j] = []
                matrix_bc.value[i][j].append(shapefile_record)

# setup our projections
in_proj = pyproj.Proj("+init=EPSG:4326")  # WGS84
out_proj = pyproj.Proj("+init=EPSG:2263", preserve_units=True)  # http://prj2epsg.org/epsg/2263

get_location_ID_udf = pyspark.sql.functions.udf(lambda lat, long: lat_long_2_location_ID(lat, long), IntegerType())

def check_lat_long_validity(input_lat, input_lon):
    if (input_lon is not None) and (input_lat is not None) and (input_lon > -180 and input_lon < 180) and (input_lat > -90 and input_lat < 90):
        return True
    else:
        return False

def check_x_y_validity(x, y):
    if (x is not None) and (y is not None) and (minx_global_bc.value <= x <= maxx_global_bc.value) and (miny_global_bc.value <= y <= maxy_global_bc.value):
        return True
    else:
        return False

# convert from our geographic coordinate system WGS84 to a New York projected coordinate system EPSG code 2263
# and get the location ID of the specified point
def lat_long_2_location_ID(input_lat, input_lon):
    not_found = -1

    if check_lat_long_validity(input_lat, input_lon):

        # the points input_lon and input_lat in the coordinate system defined by inProj are transformed
        # to x, y in the coordinate system defined by outProj
        x, y = pyproj.transform(in_proj, out_proj, input_lon, input_lat)

        if check_x_y_validity(x, y):
            x_index = int((x - minx_global_bc.value) / dx_bc.value)
            if (x == maxx_global_bc.value):
                x_index -= 1
            y_index = int((y - miny_global_bc.value) / dy_bc.value)
            if (y == maxy_global_bc.value):
                y_index -= 1

            target_point = (x, y)

            target_shape = matrix_bc.value[x_index][y_index]
            if(target_shape is None):
                return not_found
            elif(len(target_shape) == 1):
                return target_shape[0]['properties']['LocationID']
            else:
                point = shapely.geometry.Point(target_point)

                for shapefile_record in target_shape:

                    # Use Shapely to create the polygon
                    shape = shapely.geometry.asShape(shapefile_record['geometry'])

                    # Alternative: if point.within(shape)
                    if shape.contains(point):
                        return shapefile_record['properties']['LocationID']

                # If the point does not belong to any of the zones
                return not_found
        else:
            # If the point (x, y) is not within boundaries
            return not_found
    else:
        # If the latitude or longitude are not valid values
        return not_found

def v1_yellow_to_common(dataset):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("yellow").alias(taxi_company_property),
        col("lpep_pickup_datetime".lower()).alias(pickup_datetime_property),
        col("lpep_dropoff_datetime".lower()).alias(dropoff_datetime_property),
        get_location_ID_udf("pickup_latitude", "pickup_longitude").alias(pickup_location_id_property),
        get_location_ID_udf("dropoff_latitude", "dropoff_longitude").alias(dropoff_location_id_property),
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

def v2_yellow_to_common(dataset):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("yellow").alias(taxi_company_property),
        col("lpep_pickup_datetime".lower()).alias(pickup_datetime_property),
        col("lpep_dropoff_datetime".lower()).alias(dropoff_datetime_property),
        get_location_ID_udf("pickup_latitude", "pickup_longitude").alias(pickup_location_id_property),
        get_location_ID_udf("dropoff_latitude", "dropoff_longitude").alias(dropoff_location_id_property),
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

def v1_green_to_common(dataset):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("green").alias(taxi_company_property),
        col("lpep_pickup_datetime".lower()).alias(pickup_datetime_property),
        col("lpep_dropoff_datetime".lower()).alias(dropoff_datetime_property),
        get_location_ID_udf("Pickup_latitude", "Pickup_longitude").alias(pickup_location_id_property),
        get_location_ID_udf("Dropoff_latitude", "Dropoff_longitude").alias(dropoff_location_id_property),
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

def v2_green_to_common(dataset):
    dataset = dataset.select(
        col("VendorID".lower()).alias(vendor_id_property),
        lit("green").alias(taxi_company_property),
        col("lpep_pickup_datetime".lower()).alias(pickup_datetime_property),
        col("lpep_dropoff_datetime".lower()).alias(dropoff_datetime_property),
        get_location_ID_udf("Pickup_latitude", "Pickup_longitude").alias(pickup_location_id_property),
        get_location_ID_udf("Dropoff_latitude", "Dropoff_longitude").alias(dropoff_location_id_property),
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

def v3_green_to_common(dataset):
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