'''
Code for coordinate conversion exploration
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

appName = 'Parquet Converter'

def check_x_y_validity_local(x, y, minx_global, miny_global, maxx_global, maxy_global):
    if (x is not None) and (y is not None) and (minx_global <= x <= maxx_global) and (miny_global <= y <= maxy_global):
        return True
    else:
        return False

#Helper function  during the creation of the lookup matrix
#Directly uses transformed coordinates x and y
def lat_long_2_shape(x, y, shape_matrix, dx, dy, minx_global, miny_global, maxx_global, maxy_global):
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
    # Alternative: nonelist = [None] * num_tiles * num_tiles
    # matrix = np.array(nonelist)
    # matrix = np.reshape(matrix, (num_tiles, num_tiles))
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

    probe_points = []
    probe_points += [(0, 0), (dx, 0), (0, dy), (dx, dy)]
    for i in range(1, probing_density):
        for j in range(1, probing_density):
            probe_points += [(dx / i, dy / j)]

    print("Building lookup matrix")

    #Creates a refined
    matrix = np.full((num_tiles, num_tiles), None)
    for i in range(num_tiles):
        print("Processing row " + str(i))
        for j in range(num_tiles):
            matrix_element = base_matrix[i][j]
            if matrix_element is None:
                pass
            elif len(matrix_element) == 1:
                matrix[i][j] = matrix_element
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

conf = pyspark.SparkConf()
conf.set("spark.executor.memory", '1g')
conf.set('spark.executor.cores', '1')
conf.setMaster("local[2]")

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

counter = 0
not_f = 0

#Hi performance approximate version of zone association routine
#Associates ech coordinate with a zone
def lat_long_2_location_ID_hi_perf(input_lat, input_lon):
    #Importing module locally avoids its serialization
    import pyproj

    # setup our projections
    in_proj = pyproj.Proj("+init=EPSG:4326")  # WGS84
    out_proj = pyproj.Proj("+init=EPSG:2263", preserve_units=True)  # http://prj2epsg.org/epsg/2263

    #in_proj = in_proj_bc.value
    #out_proj = out_proj_bc.value

    not_found = -1

    global counter
    global not_f
    counter = counter + 1
    if(counter % 10000 == 0):
        print(counter)
        print(not_f)


    #print("Converting:")
    #print(input_lat)
    #print(input_lon)
    if (input_lon is not None) and (input_lat is not None) and (input_lon > -180 and input_lon < 180) and (input_lat > -90 and input_lat < 90):

        # the points input_lon and input_lat in the coordinate system defined by inProj are transformed
        # to x, y in the coordinate system defined by outProj
        x, y = pyproj.transform(in_proj, out_proj, input_lon, input_lat)

        #print("Converted x y")
        #print(x)
        #print(y)
        #print(minx_global)
        #print(maxx_global)
        #print(miny_global)
        #print(maxy_global)
        if (x is not None) and (y is not None) and (minx_global_bc.value <= x < maxx_global_bc.value) and (miny_global_bc.value <= y < maxy_global_bc.value):

            matrix_rows, matrix_columns = stripped_matrix_bc.value.shape
            x_index = int((x - minx_global_bc.value) / dx_bc.value)
            #We ensured coord is strictly less than maximum, so it should never go outside bounds
            #if (x_index >= matrix_rows):
            #    x_index = matrix_rows - 1
            y_index = int((y - miny_global_bc.value) / dy_bc.value)
            #if (x_index >= matrix_columns):
            #    y_index = matrix_columns - 1

           # print("current indexes")
            #print(x_index)
            #print(y_index)
            result = int(stripped_matrix_bc.value[x_index][y_index])
            if result == -1:
                not_f = not_f + 1
            return result

    #print("Wtf")
    not_f = not_f + 1
    return not_found

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

get_location_ID_udf = pyspark.sql.functions.udf(lambda lat, long: lat_long_2_location_ID(lat, long), IntegerType())
get_location_ID_hi_perf_udf = pyspark.sql.functions.udf(lat_long_2_location_ID_hi_perf, IntegerType())
vendor_id_string_2_id_udf = pyspark.sql.functions.udf(schema_conversion.vendor_string_2_id, IntegerType())
payment_type_string_2_id_udf = pyspark.sql.functions.udf(schema_conversion.payment_type_string_2_id, IntegerType())


dataset_folder = '/home/bigdata/auxiliary/'
results_folder = '/home/bigdata/auxiliary/'
conversion_folder = '/home/bigdata/auxiliary/'


#Build an entry for each archive to treat attaching the relative schema to each one
archives = []
for year in range(2013, 2014):
    if year <= 2014:
        #archives += [('green_tripdata_' + str(year), v1_green_to_common)]
        archives += [('yellow_tripdata_' + str(year), schema_conversion.v1_yellow_to_common)]

    elif year <= 2016:
        #archives += [('green_tripdata_' + str(year), v2_green_to_common)]
        archives += [('yellow_tripdata_' + str(year), schema_conversion.v2_yellow_to_common)]

    else:
        #archives += [('green_tripdata_' + str(year), v3_green_to_common)]
        archives += [('yellow_tripdata_' + str(year), schema_conversion.v3_yellow_to_common)]

#Open and convert each archive to parquet format
for archive in archives:
    print("Reading: " + archive[0])
    dataset = spark.read.parquet('file://' + conversion_folder + archive[0] + '.parquet')
    #dataset = dataset.sample(0.00001)
    converted_dataset = archive[1](dataset, get_location_ID_hi_perf_udf, vendor_id_string_2_id_udf, payment_type_string_2_id_udf)
    print("Converting: " + archive[0])
    converted_dataset.write.save(conversion_folder + archive[0] + '_common.parquet')
