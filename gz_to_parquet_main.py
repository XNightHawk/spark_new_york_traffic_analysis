'''

  ________    ______   ______     _          ____        __
 /_  __/ /   / ____/  /_  __/____(_)___     / __ \____ _/ /_____ _
  / / / /   / /        / / / ___/ / __ \   / / / / __ `/ __/ __ `/
 / / / /___/ /___     / / / /  / / /_/ /  / /_/ / /_/ / /_/ /_/ /
/_/ /_____/\____/    /_/ /_/  /_/ .___/  /_____/\__,_/\__/\__,_/
                               /_/


Authors: Willi Menapace <willi.menapace@studenti.unitn.it>
         Luca Zanella <luca.zanella-3@studenti.unitn.it>
         Daniele Giuliani <daniele.giuliani@studenti.unitn.it>

Dataset compression from tar.gz format to parquet format

Required files: tar.gz compressed dataset

IMPORTANT: Please also ensure that Spark driver memory is set in your spark configuration files
           to a sufficient amount (>= 2g), otherwise you may experience spark running out of memory while writing
           parquet results

Parameters to set:
master -> The url for the spark cluster, set to local for your convenience
conversion_folder -> Location of the dataset and results
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
for year in range(2012, 2013):
    if year <= 2012:
        #Only yellow data available in this period
        archives += [('yellow_tripdata_' + str(year), v1_schema_yellow)]
    elif year <= 2014:
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
