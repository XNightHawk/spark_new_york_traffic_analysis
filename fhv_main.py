'''

  ________    ______   ______     _          ____        __
 /_  __/ /   / ____/  /_  __/____(_)___     / __ \____ _/ /_____ _
  / / / /   / /        / / / ___/ / __ \   / / / / __ `/ __/ __ `/
 / / / /___/ /___     / / / /  / / /_/ /  / /_/ / /_/ / /_/ /_/ /
/_/ /_____/\____/    /_/ /_/  /_/ .___/  /_____/\__,_/\__/\__,_/
                               /_/


Authors: Willi Menapace <willi.menapace@studenti.unitn.it>
         Luca Zanells <luca.zanella-3@studenti.unitn.it>

Compares taxis to FHV services using a pre elaborated dataset to avoid downloading the FHV dataset

Required files: daily_trips_by_geography.csv

Parameters to set:
master -> The url for the spark cluster, set to local for your convenience
dataset_folder -> Location of the dataset
results_folder -> Location where to save results
'''

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

import schema_conversion
from schema import *
from computed_columns import *
from statistics import *
from taxi_zones_id_to_district import *

appName = 'Parquet Converter'
master = 'local[*]'

sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(appName).getOrCreate()

dataset_folder = '/home/bigdata/auxiliary/'
results_folder = '/home/bigdata/auxiliary/stats/'

fhv_schema = StructType(
    [
        StructField("company", StringType(), True),
        StructField("date", DateType(), True),
        StructField("location", StringType(), True),
        StructField("count", IntegerType(), True),
        StructField("grouping", StringType(), True),
        StructField("monthly", IntegerType(), True),
        StructField("parent", StringType(), True),
        StructField("estimated", BooleanType(), True)
    ]
)

dataset = spark.read.csv('file://' + dataset_folder + 'daily_trips_by_geography.csv', header=True, schema=fhv_schema)

dataset = dataset.groupBy(pyspark.sql.functions.year("date"), pyspark.sql.functions.dayofyear("date"), dataset["parent"]).sum("count")

dataset.cache()

dataset.toPandas().to_csv(results_folder + "fhv_vs_taxi.csv", header=True)

dataset.show()
dataset.unpersist()