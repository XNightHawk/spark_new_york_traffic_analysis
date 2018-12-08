'''
Code for performance exploration
'''

import pyspark
from pyspark.sql import SparkSession

appName = 'Parquet Converter'
master = 'local[*]'

sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(appName).getOrCreate()

dataset_folder = '/media/sf_dataset/'
dataset = spark.read.parquet(dataset_folder + 'yellow_tripdata_2014.parquet')
dataset2 = spark.read.parquet(dataset_folder + 'yellow_tripdata_2013.parquet')

passenger_count = dataset.groupBy("passenger_count").count()

print("Started execution")
#passenger_count.show()

passenger_count.toPandas().to_csv(dataset_folder + "counts.csv", header=True)