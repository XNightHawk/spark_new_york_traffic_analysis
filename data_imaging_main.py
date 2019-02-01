'''

  ________    ______   ______     _          ____        __
 /_  __/ /   / ____/  /_  __/____(_)___     / __ \____ _/ /_____ _
  / / / /   / /        / / / ___/ / __ \   / / / / __ `/ __/ __ `/
 / / / /___/ /___     / / / /  / / /_/ /  / /_/ / /_/ / /_/ /_/ /
/_/ /_____/\____/    /_/ /_/  /_/ .___/  /_____/\__,_/\__/\__,_/
                               /_/


Authors: Willi Menapace <willi.menapace@studenti.unitn.it>
         Luca Zanells <luca.zanella-3@studenti.unitn.it>

Produces high resolution images showing pickup and dropoff points contained in the dataset

Required files: Original dataset converted to parquet format

Parameters to set:
master -> The url for the spark cluster, set to local for your convenience
read_dataset_folder -> Location from which to read the dataset
dataset_folder -> Location to which to write the results
res_lat -> Resolution in pixels of the image to produce
cmap parameter -> Produces images in a different color gradient
                  Greys is used for easing post processing
'''

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

import schema_conversion
from schema import *
from computed_columns import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import multiprocessing

appName = 'Parquet Converter'
master = 'local[*]'

sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(appName).getOrCreate()


read_dataset_folder = '/home/bigdata/auxiliary/'
dataset_folder = '/home/bigdata/auxiliary/'

#Reads the dataset and transforms to a common format
#Only datasets up to 2016 contain coordinates, so later datasets are not loaded
yellow_2010 = spark.read.parquet('file://' + read_dataset_folder + 'yellow_tripdata_2010.parquet')
yellow_2010 = yellow_2010.select(yellow_2010["pickup_latitude"].alias("lat"), yellow_2010["pickup_longitude"].alias("lng"), yellow_2010["dropoff_latitude"].alias("dlat"), yellow_2010["dropoff_longitude"].alias("dlng"))
yellow_2011 = spark.read.parquet('file://' + read_dataset_folder + 'yellow_tripdata_2011.parquet')
yellow_2011 = yellow_2011.select(yellow_2011["pickup_latitude"].alias("lat"), yellow_2011["pickup_longitude"].alias("lng"), yellow_2011["dropoff_latitude"].alias("dlat"), yellow_2011["dropoff_longitude"].alias("dlng"))
yellow_2012 = spark.read.parquet('file://' + read_dataset_folder + 'yellow_tripdata_2012.parquet')
yellow_2012 = yellow_2012.select(yellow_2012["pickup_latitude"].alias("lat"), yellow_2012["pickup_longitude"].alias("lng"), yellow_2012["dropoff_latitude"].alias("dlat"), yellow_2012["dropoff_longitude"].alias("dlng"))
yellow_2013 = spark.read.parquet('file://' + read_dataset_folder + 'yellow_tripdata_2013.parquet')
yellow_2013 = yellow_2013.select(yellow_2013["pickup_latitude"].alias("lat"), yellow_2013["pickup_longitude"].alias("lng"), yellow_2013["dropoff_latitude"].alias("dlat"), yellow_2013["dropoff_longitude"].alias("dlng"))
yellow_2014 = spark.read.parquet('file://' + read_dataset_folder + 'yellow_tripdata_2014.parquet')
yellow_2014 = yellow_2014.select(yellow_2014["pickup_latitude"].alias("lat"), yellow_2014["pickup_longitude"].alias("lng"), yellow_2014["dropoff_latitude"].alias("dlat"), yellow_2014["dropoff_longitude"].alias("dlng"))
yellow_2015 = spark.read.parquet('file://' + read_dataset_folder + 'yellow_tripdata_2015.parquet')
yellow_2015 = yellow_2015.select(yellow_2015["pickup_latitude"].alias("lat"), yellow_2015["pickup_longitude"].alias("lng"), yellow_2015["dropoff_latitude"].alias("dlat"), yellow_2015["dropoff_longitude"].alias("dlng"))
yellow_2016 = spark.read.parquet('file://' + read_dataset_folder + 'yellow_tripdata_2016.parquet')
yellow_2016 = yellow_2016.select(yellow_2016["pickup_latitude"].alias("lat"), yellow_2016["pickup_longitude"].alias("lng"), yellow_2016["dropoff_latitude"].alias("dlat"), yellow_2016["dropoff_longitude"].alias("dlng"))

green_2013 = spark.read.parquet('file://' + read_dataset_folder + 'green_tripdata_2013.parquet')
green_2013 = green_2013.select(green_2013["Pickup_latitude"].alias("lat"), green_2013["Pickup_longitude"].alias("lng"), green_2013["Dropoff_latitude"].alias("dlat"), green_2013["Dropoff_longitude"].alias("dlng"))
green_2014 = spark.read.parquet('file://' + read_dataset_folder + 'green_tripdata_2014.parquet')
green_2014 = green_2014.select(green_2014["Pickup_latitude"].alias("lat"), green_2014["Pickup_longitude"].alias("lng"), green_2014["Dropoff_latitude"].alias("dlat"), green_2014["Dropoff_longitude"].alias("dlng"))
green_2015 = spark.read.parquet('file://' + read_dataset_folder + 'green_tripdata_2015.parquet')
green_2015 = green_2015.select(green_2015["Pickup_latitude"].alias("lat"), green_2015["Pickup_longitude"].alias("lng"), green_2015["Dropoff_latitude"].alias("dlat"), green_2015["Dropoff_longitude"].alias("dlng"))
green_2016 = spark.read.parquet('file://' + read_dataset_folder + 'green_tripdata_2016.parquet')
green_2016 = green_2016.select(green_2016["Pickup_latitude"].alias("lat"), green_2016["Pickup_longitude"].alias("lng"), green_2016["Dropoff_latitude"].alias("dlat"), green_2016["Dropoff_longitude"].alias("dlng"))

yellow_dataset = yellow_2010.union(yellow_2011).union(yellow_2012).union(yellow_2013).union(yellow_2014).union(yellow_2015).union(yellow_2016)

lat_low = 40.437467
lng_low = -74.354857
lng_high = -73.708329
#Make sure it is a square boundary
lat_high = lat_low + (lng_high - lng_low)

res_lat = 5000
divide_factor = (lng_high - lng_low) / res_lat

yellow_dataset = yellow_dataset.filter(yellow_dataset["lat"] > lat_low)
yellow_dataset = yellow_dataset.filter(yellow_dataset["lat"] < lat_high)
yellow_dataset = yellow_dataset.filter(yellow_dataset["lng"] > lng_low)
yellow_dataset = yellow_dataset.filter(yellow_dataset["lng"] < lng_high)
yellow_dataset = yellow_dataset.filter(yellow_dataset["dlat"] > lat_low)
yellow_dataset = yellow_dataset.filter(yellow_dataset["dlat"] < lat_high)
yellow_dataset = yellow_dataset.filter(yellow_dataset["dlng"] > lng_low)
yellow_dataset = yellow_dataset.filter(yellow_dataset["dlng"] < lng_high)

green_dataset = green_2013.union(green_2014).union(green_2015).union(green_2016)
green_dataset = green_dataset.filter(green_dataset["lat"] > lat_low)
green_dataset = green_dataset.filter(green_dataset["lat"] < lat_high)
green_dataset = green_dataset.filter(green_dataset["lng"] > lng_low)
green_dataset = green_dataset.filter(green_dataset["lng"] < lng_high)
green_dataset = green_dataset.filter(green_dataset["dlat"] > lat_low)
green_dataset = green_dataset.filter(green_dataset["dlat"] < lat_high)
green_dataset = green_dataset.filter(green_dataset["dlng"] > lng_low)
green_dataset = green_dataset.filter(green_dataset["dlng"] < lng_high)

dataset = yellow_dataset.union(green_dataset)

dataset = dataset.filter(dataset["lat"] > lat_low)
dataset = dataset.filter(dataset["lat"] < lat_high)
dataset = dataset.filter(dataset["lng"] > lng_low)
dataset = dataset.filter(dataset["lng"] < lng_high)

pickup_dataset = dataset.select((((dataset["lat"] - (lat_low)) / divide_factor).cast(IntegerType()).alias("lat_index")), (((dataset["lng"] - (lng_low)) / divide_factor).cast(IntegerType()).alias("lng_index"))).groupBy("lat_index", "lng_index").count()
dropoff_dataset = dataset.select((((dataset["dlat"] - (lat_low)) / divide_factor).cast(IntegerType()).alias("lat_index")), (((dataset["dlng"] - (lng_low)) / divide_factor).cast(IntegerType()).alias("lng_index"))).groupBy("lat_index", "lng_index").count()

yellow_pickup_dataset = yellow_dataset.select((((yellow_dataset["lat"] - (lat_low)) / divide_factor).cast(IntegerType()).alias("lat_index")), (((yellow_dataset["lng"] - (lng_low)) / divide_factor).cast(IntegerType()).alias("lng_index"))).groupBy("lat_index", "lng_index").count()
yellow_dropoff_dataset = yellow_dataset.select((((yellow_dataset["dlat"] - (lat_low)) / divide_factor).cast(IntegerType()).alias("lat_index")), (((yellow_dataset["dlng"] - (lng_low)) / divide_factor).cast(IntegerType()).alias("lng_index"))).groupBy("lat_index", "lng_index").count()

green_pickup_dataset = green_dataset.select((((green_dataset["lat"] - (lat_low)) / divide_factor).cast(IntegerType()).alias("lat_index")), (((green_dataset["lng"] - (lng_low)) / divide_factor).cast(IntegerType()).alias("lng_index"))).groupBy("lat_index", "lng_index").count()
green_dropoff_dataset = green_dataset.select((((green_dataset["dlat"] - (lat_low)) / divide_factor).cast(IntegerType()).alias("lat_index")), (((green_dataset["dlng"] - (lng_low)) / divide_factor).cast(IntegerType()).alias("lng_index"))).groupBy("lat_index", "lng_index").count()

#Produces CSV intermediate results
print("Elaborating 1")
pickup_dataset.toPandas().to_csv(dataset_folder + "pickup_image_data.csv", header=True)
print("Elaborating 2")
dropoff_dataset.toPandas().to_csv(dataset_folder + "dropoff_image_data.csv", header=True)
print("Elaborating 3")
yellow_pickup_dataset.toPandas().to_csv(dataset_folder + "yellow_pickup_image_data.csv", header=True)
print("Elaborating 4")
yellow_dropoff_dataset.toPandas().to_csv(dataset_folder + "yellow_dropoff_image_data.csv", header=True)
print("Elaborating 5")
green_pickup_dataset.toPandas().to_csv(dataset_folder + "green_pickup_image_data.csv", header=True)
print("Elaborating 6")
green_dropoff_dataset.toPandas().to_csv(dataset_folder + "green_dropoff_image_data.csv", header=True)


image_names = ["pickup_image_data", "dropoff_image_data", "yellow_pickup_image_data", "yellow_dropoff_image_data", "green_pickup_image_data", "green_dropoff_image_data"]

def elaborate_image(image_name):
    print("Elaborating " + image_name)
    image = pd.read_csv(dataset_folder + image_name + ".csv")
    counts = np.zeros((res_lat, res_lat))

    for index, row in image.iterrows():
        if index % 5000 == 0:
            print(index)
        counts[int(row["lat_index"]), int(row["lng_index"])] = int(row["count"])

    np.save(dataset_folder + image_name, counts)

#Translates intermediate results to image pixels
pool = multiprocessing.Pool(6)
pool.map(elaborate_image, image_names)

#Produces images
for image_name in image_names:

    counts = np.load(dataset_folder + image_name + ".npy")
    counts = np.log(counts + 1)
    plt.imsave(dataset_folder + image_name +".png", counts, dpi=4000, cmap="Greys")
