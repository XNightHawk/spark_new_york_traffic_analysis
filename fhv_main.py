import pyspark
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler, OneHotEncoderEstimator

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