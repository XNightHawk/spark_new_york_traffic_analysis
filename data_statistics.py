import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions

import schema_conversion
from schema import *

appName = 'Parquet Converter'
master = 'local[*]'

sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(appName).getOrCreate()

dataset_folder = '/media/sf_dataset/'
results_folder = '/media/sf_dataset/statistics/'
yellow_2018 = spark.read.parquet(dataset_folder + 'yellow_tripdata_2018.parquet')
yellow_2017 = spark.read.parquet(dataset_folder + 'yellow_tripdata_2017.parquet')

yellow_2018 = schema_conversion.v3_yellow_to_common(yellow_2018)
yellow_2017 = schema_conversion.v3_yellow_to_common(yellow_2017)

dataset = yellow_2017.union(yellow_2018)

dataset.printSchema()
dataset.show()

"""
#Calculate pickup hour distribution
time_distribution = dataset.select(pyspark.sql.functions.hour(pickup_datetime_property).alias("pickup_hour")).groupBy("pickup_hour").count()
time_distribution.toPandas().to_csv(results_folder + "pickup_hour_distribution.csv", header=True)
time_distribution.show()
"""
"""
#Calculate pickup hour distribution
time_distribution = dataset.select(pyspark.sql.functions.dayofyear(pickup_datetime_property).alias("pickup_day")).groupBy("pickup_day").count()
time_distribution.toPandas().to_csv(results_folder + "pickup_day_distribution.csv", header=True)
time_distribution.show()
"""
"""
#Calculate pickup month distribution
time_distribution = dataset.select(pyspark.sql.functions.month(pickup_datetime_property).alias("pickup_month")).groupBy("pickup_month").count()
time_distribution.toPandas().to_csv(results_folder + "pickup_month_distribution.csv", header=True)
time_distribution.show()
"""

"""
#Calculate pickup location distribution
location_distribution = dataset.groupBy(pickup_location_id_property).count()
location_distribution.toPandas().to_csv(results_folder + "pickup_id_distribution.csv", header=True)
location_distribution.show()
"""

'''
#Calculate pickup location distribution
location_distribution = dataset.groupBy(dropoff_location_id_property).count()
location_distribution.toPandas().to_csv(results_folder + "dropoff_id_distribution.csv", header=True)
location_distribution.show()
'''

#Calculate pickup location distribution
passenger_count_distribution = dataset.groupBy(passenger_count_property).count()
passenger_count_distribution.toPandas().to_csv(results_folder + "passenger_count_distribution.csv", header=True)
passenger_count_distribution.show()