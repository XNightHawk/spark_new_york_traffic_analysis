'''
Statistical data analysis routines
'''

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

import schema_conversion
from schema import *
from computed_columns import *

#Filters dataset returning the filtered one
def clean(dataset):

    #Not used columns which contain many nulls
    dataset = dataset.drop(store_and_forward_flag_property)
    dataset = dataset.drop(vendor_id_property)

    #No nulls in the dataset
    dataset = dataset.dropna()

    dataset = dataset.filter((dataset[taxi_company_property] == "yellow") | (dataset[taxi_company_property] == "green"))

    dataset = dataset.filter((pyspark.sql.functions.year(dataset[pickup_datetime_property]) >= 2010) & (pyspark.sql.functions.year(dataset[pickup_datetime_property]) <= 2018))
    dataset = dataset.filter((pyspark.sql.functions.year(dataset[dropoff_datetime_property]) >= 2010) & (pyspark.sql.functions.year(dataset[dropoff_datetime_property]) <= 2018))

    dataset = dataset.filter((trip_duration_minutes_column(dataset) >= 0) & (trip_duration_minutes_column(dataset) <= 12 * 60))

    dataset = dataset.filter((dataset[pickup_location_id_property] >= 1) & (dataset[pickup_location_id_property] <= 265))
    dataset = dataset.filter((dataset[dropoff_location_id_property] >= 1) & (dataset[dropoff_location_id_property] <= 265))

    dataset = dataset.filter((dataset[passenger_count_property] >= 1) & (dataset[passenger_count_property] <= 6))
    dataset = dataset.filter((dataset[trip_distance_property] >= 0) & (dataset[trip_distance_property] <= 110))
    dataset = dataset.filter((dataset[ratecode_id_property] >= 1) & (dataset[ratecode_id_property] <= 6))
    dataset = dataset.filter((dataset[fare_amount_property] >= 0) & (dataset[fare_amount_property] <= 200))
    dataset = dataset.filter((dataset[tolls_amount_property] >= 0) & (dataset[tolls_amount_property] <= 120))
    dataset = dataset.filter((dataset[total_amount_property] >= 0) & (dataset[total_amount_property] <= 250))
    dataset = dataset.filter((dataset[mta_tax_property] >= 0) & (dataset[mta_tax_property] <= 1))
    dataset = dataset.filter((dataset[improvement_surcharge_property] >= 0) & (dataset[improvement_surcharge_property] <= 0.5))
    dataset = dataset.filter((dataset[extra_property] >= 0) & (dataset[extra_property] <= 1))
    dataset = dataset.filter((dataset[tip_amount_property] >= 0) & (dataset[tip_amount_property] <= 60))
    dataset = dataset.filter((dataset[payment_type_property] >= 1) & (dataset[payment_type_property] <= 6))

    return dataset

appName = 'Parquet Converter'
master = 'local[*]'

sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(appName).getOrCreate()


read_dataset_folder = '/media/sf_dataset/'
dataset_folder = '/home/bigdata/auxiliary/'
results_folder = '/home/bigdata/auxiliary/stats/'

#Build an entry for each archive to treat attaching the relative schema conversion routine to each one
archives = []
for year in range(2010, 2019):
    if year <= 2014:
        if year >= 2013:
            archives += ['green_tripdata_' + str(year)]
        archives += ['yellow_tripdata_' + str(year)]

    elif year <= 2016:
        archives += ['green_tripdata_' + str(year)]
        archives += ['yellow_tripdata_' + str(year)]

    else:
        archives += ['green_tripdata_' + str(year)]
        archives += ['yellow_tripdata_' + str(year)]

dataset = None

#Open and convert each archive to parquet format
for archive in archives:
    print("Reading: " + archive)

    current_dataset = spark.read.parquet('file://' + read_dataset_folder + archive + '_common.parquet')
    if dataset is None:
        dataset = current_dataset
    else:
        dataset = dataset.union(current_dataset)


print('Original dataset rows: ' + str(dataset.count()))

cleaned_dataset = clean(dataset)
cleaned_dataset.write.parquet('file://' + dataset_folder + 'clean_dataset.parquet')
cleaned_dataset = spark.read.parquet('file://' + dataset_folder + 'clean_dataset.parquet')

cleaned_dataset.show()
print('Cleaned dataset rows: ' + str(cleaned_dataset.count()))

