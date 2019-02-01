'''

  ________    ______   ______     _          ____        __
 /_  __/ /   / ____/  /_  __/____(_)___     / __ \____ _/ /_____ _
  / / / /   / /        / / / ___/ / __ \   / / / / __ `/ __/ __ `/
 / / / /___/ /___     / / / /  / / /_/ /  / /_/ / /_/ / /_/ /_/ /
/_/ /_____/\____/    /_/ /_/  /_/ .___/  /_____/\__,_/\__/\__,_/
                               /_/


Authors: Willi Menapace <willi.menapace@studenti.unitn.it>
         Luca Zanells <luca.zanella-3@studenti.unitn.it>

Converts parquet files in the original schema to parquet files with the common schema performing
also coordinate conversion
The code is non trivial, so the reader may want to refer to the corresponding section of the report
to understand the flow of execution
You may want to skip the calculation of the lookup matrix by ensuring lookup_matrix_1000_3.npy is
present in the current directory

IMPORTANT: Please also ensure that Spark driver memory is set in your spark configuration files
           to a sufficient amount (>= 2g), otherwise you may experience spark running out of memory while writing
           parquet results

Required files: Original dataset in parquet format
                NYC taxi zones shapefile, already available in the repository

Parameters to set:
master -> The url for the spark cluster, set to local for your convenience
dataset_folder -> Location of the dataset
results_folder -> Location where to save results
shapefile_path -> Location where to read the NYC taxi zones shapefile
'''


import os
import random
import fiona
import shapely
import pyproj
import numpy as np

import schema_conversion

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def check_x_y_validity_local(x, y, minx_global, miny_global, maxx_global, maxy_global):
    if (x is not None) and (y is not None) and (minx_global <= x <= maxx_global) and (miny_global <= y <= maxy_global):
        return True
    else:
        return False


def lat_long_2_shape(x, y, shape_matrix, dx, dy, minx_global, miny_global, maxx_global, maxy_global):
    '''
    Helper function for the creation of the lookup matrix.
    Associates coordinates to a location using a lookup matrix
    Directly uses transformed coordinates x and y
    '''

    not_found = None

    if check_x_y_validity_local(x, y, minx_global, miny_global, maxx_global, maxy_global):
        x_index = int((x - minx_global) / dx)
        if (x == maxx_global):
            x_index -= 1
        y_index = int((y - miny_global) / dy)
        if (y == maxy_global):
            y_index -= 1

        target_point = (x, y)

        target_shape = shape_matrix[x_index][y_index]
        if(target_shape is None):
            return not_found
        elif(len(target_shape) == 1):
            return target_shape[0]
        else:
            point = shapely.geometry.Point(target_point)

            for shapefile_record in target_shape:

                # Use Shapely to create the polygon
                shape = shapely.geometry.asShape(shapefile_record['geometry'])

                # Alternative: if point.within(shape)
                if shape.contains(point):
                    return shapefile_record

            # If the point does not belong to any of the zones
            return not_found
    else:
        # If the point (x, y) is not within boundaries
        return not_found

def initialize_lookup_matrix(zones_shapes, num_tiles, probing_density, save_location='', load_location=''):
    '''
    Creates a refined lookup matrix for coordinates conversion to locations
    Accepts a shapefile, the number of cells in each dimension of the matrix, a density for probing points and a location
    where to save or load the created matrix for better performance
    '''

    minx_global, miny_global, maxx_global, maxy_global = zones_shapes.bounds

    # Width of each tile
    dx = (maxx_global - minx_global) / num_tiles

    # Height of each tile
    dy = (maxy_global - miny_global) / num_tiles

    # setup our projections
    in_proj = pyproj.Proj("+init=EPSG:4326")  # WGS84
    out_proj = pyproj.Proj("+init=EPSG:2263", preserve_units=True)  # http://prj2epsg.org/epsg/2263

    #If matrix already exists load it
    if load_location and os.path.isfile(load_location):
        print("Loading lookup matrix")
        matrix = np.load(load_location)
        print("Lookup matrix loaded")
        return (matrix, dx, dy, minx_global, miny_global, maxx_global, maxy_global, in_proj, out_proj)

    # Create a num_tiles * num_tiles matrix of None elements
    # It is a non refined matrix used to speed up probing operations
    base_matrix = np.full((num_tiles, num_tiles), None)

    # Populate matrix
    for shapefile_record in zones_shapes:
        # Use Shapely to create the polygon
        shape = shapely.geometry.asShape(shapefile_record['geometry'])
        minx, miny, maxx, maxy = shape.bounds
        minx_index = int((minx - minx_global) / dx)
        maxx_index = int((maxx - minx_global) / dx)
        if (maxx == maxx_global):
            maxx_index -= 1
        miny_index = int((miny - miny_global) / dy)
        maxy_index = int((maxy - miny_global) / dy)
        if (maxy == maxy_global):
            maxy_index -= 1

        for i in range(minx_index, maxx_index + 1):
            for j in range(miny_index, maxy_index + 1):
                if(base_matrix[i][j] is None):
                    base_matrix[i][j] = []
                base_matrix[i][j].append(shapefile_record)

    #Initialize probe points according to probing density
    probe_points = []
    probe_points += [(0, 0), (dx, 0), (0, dy), (dx, dy)]
    for i in range(1, probing_density):
        for j in range(1, probing_density):
            probe_points += [(dx / i, dy / j)]

    print("Building lookup matrix")

    #Creates a refined lookup matrix starting from the non refined one
    matrix = np.full((num_tiles, num_tiles), None)
    for i in range(num_tiles):
        print("Processing row " + str(i))
        for j in range(num_tiles):
            matrix_element = base_matrix[i][j]

            #If no elements or exactly one are present, there is nothing to do
            if matrix_element is None:
                pass
            elif len(matrix_element) == 1:
                matrix[i][j] = matrix_element
            #If there are many possible locations, then use probing points to determine the ones that
            #are effectively in the current cell
            else:
                new_matrix_elements = []
                for current_probe in probe_points:

                    probe_shape = lat_long_2_shape(current_probe[0] + minx_global + i * dx, current_probe[1] + miny_global + j * dy, base_matrix, dx, dy, minx_global, miny_global, maxx_global, maxy_global)

                    if (not (probe_shape is None)) and probe_shape not in new_matrix_elements:
                        new_matrix_elements.append(probe_shape)
                #If no shapes are found leave null instead of the empty list
                if len(new_matrix_elements) > 0:
                    matrix[i][j] = new_matrix_elements

    print("Lookup matrix built")
    if save_location:
        np.save(save_location, matrix)

    return (matrix, dx, dy, minx_global, miny_global, maxx_global, maxy_global, in_proj, out_proj)

def initialize_stripped_lookup_matrix(matrix):
    '''
    Builds an approximate lookup matrix that to each cell associates exactly one locaiton id, -1 if no zone is present
    '''

    print("Stripping lookup matrix")
    rows, columns = matrix.shape

    #Initialize each element to the unassigned element
    stripped_matrix = np.full((rows, columns), -1)

    for i in range(rows):
        for j in range(columns):
            current_zones = matrix[i][j]
            #Check that it is not null and that it has at least one element
            if current_zones:
                random.shuffle(current_zones) #get a random zone between the possible ones
                stripped_matrix[i][j] = current_zones[0]['properties']['LocationID']

    print("Lookup matrix stripped")
    return stripped_matrix

master = "local[*]"
appName = 'Parquet Common Schema Converter'

#Please also ensure that Spark driver memory is set in the configuration files
#to a sufficient amount (>= 2g)
conf = pyspark.SparkConf()
conf.set("spark.executor.memory", '1g')
conf.set('spark.executor.cores', '1')
conf.setMaster(master)

sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.appName(appName).getOrCreate()

#Reads shapefile
shapefile_path = 'docs/taxi_zones/taxi_zones.shp'
taxi_zones_shapes = fiona.open(shapefile_path)

#Number of rows and columns in the lookup matrix
num_tiles = 1000
#Specifies a precision factor for approximate region to lookup matrix associations
#Increases qudratically the time needed for lookup matrix computation
probing_density = 3

#Initialize coordinate conversion lookup matrix
matrix, dx, dy, minx_global, miny_global, maxx_global, maxy_global, in_proj, out_proj = initialize_lookup_matrix(taxi_zones_shapes, num_tiles, probing_density,
                                                                                                                 save_location='lookup_matrix_' + str(num_tiles) + '_' + str(probing_density) + '.npy',
                                                                                                                 load_location='lookup_matrix_' + str( num_tiles) + '_' + str(probing_density) + '.npy')
#Creates an approximate matrix that directly associates a matrix sector with an id
stripped_matrix = initialize_stripped_lookup_matrix(matrix)

#Initialize broadcast variables
matrix_bc = spark.sparkContext.broadcast(matrix)
stripped_matrix_bc = spark.sparkContext.broadcast(stripped_matrix)
minx_global_bc = spark.sparkContext.broadcast(minx_global)
miny_global_bc = spark.sparkContext.broadcast(miny_global)
maxx_global_bc = spark.sparkContext.broadcast(maxx_global)
maxy_global_bc = spark.sparkContext.broadcast(maxy_global)
dx_bc = spark.sparkContext.broadcast(dx)
dy_bc = spark.sparkContext.broadcast(dy)
in_proj_bc = spark.sparkContext.broadcast(in_proj)
out_proj_bc = spark.sparkContext.broadcast(out_proj)


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

def lat_long_2_location_ID_hi_perf(input_lat, input_lon):
    '''
    Hi performance approximate version of zone association routine
    Associates ech coordinate with a zone
    '''

    #Importing module locally avoids its serialization
    import pyproj

    # Coordinate conversion objects.
    # They are not correctly serialized among workers, so they are initialized each time
    in_proj = pyproj.Proj("+init=EPSG:4326")  # WGS84
    out_proj = pyproj.Proj("+init=EPSG:2263", preserve_units=True)  # http://prj2epsg.org/epsg/2263

    not_found = -1

    if (input_lon is not None) and (input_lat is not None) and (input_lon > -180 and input_lon < 180) and (input_lat > -90 and input_lat < 90):

        # the points input_lon and input_lat in the coordinate system defined by inProj are transformed
        # to x, y in the coordinate system defined by outProj
        x, y = pyproj.transform(in_proj, out_proj, input_lon, input_lat)

        if (x is not None) and (y is not None) and (minx_global_bc.value <= x < maxx_global_bc.value) and (miny_global_bc.value <= y < maxy_global_bc.value):

            matrix_rows, matrix_columns = stripped_matrix_bc.value.shape
            x_index = int((x - minx_global_bc.value) / dx_bc.value)
            y_index = int((y - miny_global_bc.value) / dy_bc.value)

            result = int(stripped_matrix_bc.value[x_index][y_index])
            return result

    return not_found

get_location_ID_hi_perf_udf = pyspark.sql.functions.udf(lat_long_2_location_ID_hi_perf, IntegerType())
vendor_id_string_2_id_udf = pyspark.sql.functions.udf(schema_conversion.vendor_string_2_id, IntegerType())
payment_type_string_2_id_udf = pyspark.sql.functions.udf(schema_conversion.payment_type_string_2_id, IntegerType())

dataset_folder = '/home/bigdata/auxiliary/'
conversion_folder = '/home/bigdata/auxiliary/'

#Build an entry for each archive to treat attaching the relative schema conversion routine to each one
archives = []
for year in range(2010, 2019):
    if year <= 2014:
        if year >= 2013:
            archives += [('green_tripdata_' + str(year), schema_conversion.v1_green_to_common)]
        archives += [('yellow_tripdata_' + str(year), schema_conversion.v1_yellow_to_common)]

    elif year <= 2016:
        archives += [('green_tripdata_' + str(year), schema_conversion.v2_green_to_common)]
        archives += [('yellow_tripdata_' + str(year), schema_conversion.v2_yellow_to_common)]

    else:
        archives += [('green_tripdata_' + str(year), schema_conversion.v3_green_to_common)]
        archives += [('yellow_tripdata_' + str(year), schema_conversion.v3_yellow_to_common)]

#Open and convert each archive to parquet format
for archive in archives:
    print("Reading: " + archive[0])
    dataset = spark.read.parquet('file://' + conversion_folder + archive[0] + '.parquet')

    converted_dataset = archive[1](dataset, get_location_ID_hi_perf_udf, vendor_id_string_2_id_udf, payment_type_string_2_id_udf)
    print("Converting: " + archive[0])
    converted_dataset.write.save(conversion_folder + archive[0] + '_common.parquet')


