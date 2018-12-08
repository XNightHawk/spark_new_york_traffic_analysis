'''
Compression utility from .tar.gz format to parquet for original data
'''

import pyspark
from pyspark.sql import SparkSession

from original_schemas import *
from pyspark.sql.types import *

appName = 'Parquet Converter'
master = 'local[*]'

sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(appName).getOrCreate()

conversion_folder = '/media/sf_dataset/'

#Build an entry for each archive to treat attaching the relative schema to each one
archives = []
for year in range(2013, 2019):
    if year <= 2014:
        archives += [('green_tripdata_' + str(year), v1_schema_green)]
        archives += [('yellow_tripdata_' + str(year), v1_schema_yellow)]

    elif year <= 2016:
        archives += [('green_tripdata_' + str(year), v2_schema_green)]
        archives += [('yellow_tripdata_' + str(year), v2_schema_yellow)]

    else:
        archives += [('green_tripdata_' + str(year), v3_schema_green)]
        archives += [('yellow_tripdata_' + str(year), v3_schema_yellow)]

#Open and convert each archive to parquet format
for archive in archives:
    print("Reading: " + archive[0])
    dataset = spark.read.csv('file://' + conversion_folder + archive[0] + '.tar.gz', header=True, schema=archive[1])
    dataset.printSchema()
    dataset.show()
    #Converts column names to lowercase
    for column in dataset.columns:
        dataset = dataset.withColumnRenamed(column, str(column).strip().lower())
    dataset.printSchema()
    dataset.show(10)
    print("Writing: " + archive[0])
    dataset.write.save(conversion_folder + archive[0] + '.parquet')
