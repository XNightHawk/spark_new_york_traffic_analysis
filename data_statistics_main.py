'''
Statistical data analysis routines
'''

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

#Whether to use the cleaned dataset or the uncleaned one
clustered_dataset = True

#Whether to perform a per cluster analysis or a global one
clustered_analysis = False

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

if not clustered_dataset:
    #Open and convert each archive to parquet format
    for archive in archives:
        print("Reading: " + archive)

        current_dataset = spark.read.parquet('file://' + dataset_folder + archive + '_common.parquet')
        if dataset is None:
            dataset = current_dataset
        else:
            dataset = dataset.union(current_dataset)

else:
    dataset = spark.read.parquet('file://' + dataset_folder + 'clustered_dataset.parquet')

#dataset.printSchema()
#dataset.show()

#compute_pickup_hour_distribution(dataset, results_folder + "pickup_hour_dist.csv", True, clustered_analysis)
#compute_pickup_yearday_distribution(dataset, results_folder + "pickup_yearday_dist.csv", True, clustered_analysis)
#compute_pickup_weekday_distribution(dataset, results_folder + "pickup_weekday_dist.csv", True, clustered_analysis)
#compute_pickup_month_distribution(dataset, results_folder + "pickup_month_dist.csv", True, clustered_analysis)
#compute_pickup_location_id_distribution(dataset, results_folder + "pickup_location_id_dist.csv", True, clustered_analysis)
#compute_dropoff_location_id_distribution(dataset, results_folder + "dropoff_location_id_dist.csv", True, clustered_analysis)
#compute_passenger_count_distribution(dataset, results_folder + "passenger_count_dist.csv", True, clustered_analysis)
#compute_trip_duration_distribution(dataset, results_folder + "trip_duration_dist.csv", True, clustered_analysis)
#compute_trip_distance_by_pickup_hour_distribution(dataset, results_folder + "trip_distance_by_pickup_hour_dist.csv", True, clustered_analysis)
#compute_trip_duration_by_pickup_hour_distribution(dataset, results_folder + "trip_duration_by_pickup_hour_dist.csv", True, clustered_analysis)
#compute_average_speed_by_pickup_hour_distribution(dataset, results_folder + "avg_speed_by_pickup_hour_dist.csv", True, clustered_analysis)
#compute_total_amount_by_pickup_hour_distribution(dataset, results_folder + "total_amount_by_pickup_hour_dist.csv", True, clustered_analysis)
#compute_average_total_amount_by_pickup_hour(dataset, results_folder + "avg_total_amount_by_pickup_hour.csv", True, clustered_analysis)
#compute_average_duration_by_pickup_location(dataset, results_folder + "avg_duration_by_pickup_location.csv", True, clustered_analysis)
#compute_average_distance_by_pickup_location(dataset, results_folder + "avg_distance_by_pickup_location.csv", True, clustered_analysis)
#compute_average_speed_by_pickup_location(dataset, results_folder + "avg_speed_by_pickup_location.csv", True, clustered_analysis)
#compute_average_total_amount_by_pickup_location(dataset, results_folder + "avg_total_amount_by_pickup_location.csv", True, clustered_analysis)
#compute_pickup_location_distribution(dataset, results_folder + "pickup_location_distr.csv", True, clustered_analysis)
#compute_pickup_location_by_pickup_hour_distribution(dataset, results_folder + "pickup_location_by_pickup_hour_distr.csv", True, clustered_analysis)
#compute_dropoff_location_distribution(dataset, results_folder + "dropoff_location_distr.csv", True, clustered_analysis)
#compute_dropoff_location_by_pickup_hour_distribution(dataset, results_folder + "dropoff_location_by_pickup_hour_distr.csv", True, clustered_analysis)
#compute_pickup_dropoff_location_by_pickup_hour_distribution(dataset, results_folder + "pickup_dropoff_location_by_pickup_hour_distr.csv", True, clustered_analysis)
#compute_passenger_count_distribution(dataset, results_folder + "passenger_count_distr.csv", True, clustered_analysis)
#compute_trip_distance_distribution(dataset, results_folder + "trip_distance_distr.csv", True, clustered_analysis)
#compute_ratecode_distribution(dataset, results_folder + "ratecode_distr.csv", True, clustered_analysis)
#compute_fare_amount_distribution(dataset, results_folder + "fare_amount_distr.csv", True, clustered_analysis)
#compute_average_fare_amount_percentage_by_year(dataset, results_folder + "avg_fare_amount_percentage.csv", True, clustered_analysis)
#compute_tolls_amount_distribution(dataset, results_folder + "tolls_amount_distr.csv", True, clustered_analysis)
#compute_total_amount_distribution(dataset, results_folder + "total_amount_distr.csv", True, clustered_analysis)
#compute_profits_by_year(dataset, results_folder + "profits_by_year.csv", True, clustered_analysis)
#compute_monthly_profit_percentage_by_year_and_month(dataset, results_folder + "monthly_profit_by_year.csv", True, clustered_analysis)
#compute_average_total_amount_by_year(dataset, results_folder + "avg_total_amount_by_year.csv", True, clustered_analysis)
#compute_average_total_amount_by_month(dataset, results_folder + "avg_total_amount_by_month.csv", True, clustered_analysis)
#compute_mta_tax_profits_by_year(dataset, results_folder + "mta_tax_by_year.csv", True, clustered_analysis)
#compute_improvement_surcharge_profits_by_year(dataset, results_folder + "improvement_surcharge_by_year.csv", True, clustered_analysis)
#compute_average_extra_by_hour(dataset, results_folder + "avg_extra_by_hour.csv", True, clustered_analysis)
#compute_tips_distribution(dataset, results_folder + "tips_distr.csv", True, clustered_analysis)
#compute_average_tip_percentage(dataset, results_folder + "avg_tip_percentage.csv", True, clustered_analysis)
#compute_average_tip_percentage_by_year(dataset, results_folder + "avg_tip_percentage_by_year.csv", True, clustered_analysis)
#compute_payment_type_distribution(dataset, results_folder + "payment_type_distr.csv", True, clustered_analysis)
#compute_payment_type_by_year_distribution(dataset, results_folder + "payment_type_distr.csv", True, clustered_analysis)

#compute_travels_by_year_and_company(dataset, results_folder + "travels_by_year_and company.csv", True, clustered_analysis)
#compute_profits_by_year_and_company(dataset, results_folder + "profits_by_year_and company.csv", True, clustered_analysis)
#compute_trip_distance_by_year_and_company_distribution(dataset, results_folder + "trip_distance_by_year_and company_distr.csv", True, clustered_analysis)
#compute_passenger_count_by_year_and_company_distribution(dataset, results_folder + "passenger_count_by_year_and_company_distr.csv", True, clustered_analysis)
#compute_pickup_location_id_by_company_distribution(dataset, results_folder + "pickup_location_by_company_distr.csv", True, clustered_analysis)


#compute_mta_tax_distribution(dataset, results_folder + "mta_tax_distr.csv", True, clustered_analysis)
#compute_improvement_surcharge_distribution(dataset, results_folder + "improvement_surcharge_distr.csv", True, clustered_analysis)
#compute_extra_distribution(dataset, results_folder + "extra_distr.csv", True, clustered_analysis)
#compute_total_amount_distribution(dataset, results_folder + "total_amount_distr.csv", True, clustered_analysis)

#for current_column in [(dataset[payment_type_property], "payment_type"), (dataset[ratecode_id_property], "ratecode_id"), (dayofweek(dataset[dropoff_datetime_property]), 'dayofweek'), (hour(dataset[pickup_datetime_property]), "pickup_hour"), (hour(dataset[dropoff_datetime_property]), "dropoff_hour"), (dataset[passenger_count_property], "passenger_count"), (speed_column(dataset, 'speed'), "speed"), (dataset[trip_distance_property], "distance"), (dataset[fare_amount_property], "fare_amount"), (dataset[tolls_amount_property], 'tolls_amount'), (dataset[tip_amount_property], 'tip_amount')]:

#    compute_mean(dataset, results_folder + current_column[1] + "_mean.csv", current_column[0], True, clustered_analysis)
#    compute_variance(dataset, results_folder + current_column[1] + "_variance.csv", current_column[0], True, clustered_analysis)

#Traffic movement analysis

#for zone in [("manhattan", manhattan_ids), ("bronx", bronx_ids), ("brooklyn", brooklyn_ids), ("queens", queens_ids), ("staten_island", staten_island_ids)]:
#    zone_dataset = dataset.filter(dataset[pickup_location_id_property].isin(zone[1]))
#    compute_dropoff_location_id_distribution(zone_dataset, results_folder + zone[0] + "_dropoff_location_id_dist.csv", True, clustered_analysis)
#    compute_dropoff_location_by_pickup_hour_distribution(zone_dataset, results_folder + zone[0] + "_dropoff_location_id_by_pickup_hour_dist.csv", True, clustered_analysis)
