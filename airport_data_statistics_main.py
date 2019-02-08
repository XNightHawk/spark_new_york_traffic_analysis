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

Statistical data analysis of airport data

Required files: Clustered dataset

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
results_folder = '/home/bigdata/auxiliary/airport_stats/'

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

#Dataset without airport trip filtering
original_dataset = dataset

#Filters data relative to airports only
#3 is the newyark fare rate
airport_locations = [132, 138]
dataset = dataset.filter(dataset[dropoff_location_id_property].isin(airport_locations) | dataset[pickup_location_id_property].isin(airport_locations) | (dataset[ratecode_id_property] == 3))

compute_pickup_hour_distribution(dataset, results_folder + "pickup_hour_dist.csv", True, clustered_analysis)
compute_pickup_yearday_distribution(dataset, results_folder + "pickup_yearday_dist.csv", True, clustered_analysis)
compute_pickup_weekday_distribution(dataset, results_folder + "pickup_weekday_dist.csv", True, clustered_analysis)
compute_pickup_month_distribution(dataset, results_folder + "pickup_month_dist.csv", True, clustered_analysis)
compute_pickup_location_id_distribution(dataset, results_folder + "pickup_location_id_dist.csv", True, clustered_analysis)
compute_dropoff_location_id_distribution(dataset, results_folder + "dropoff_location_id_dist.csv", True, clustered_analysis)
compute_passenger_count_distribution(dataset, results_folder + "passenger_count_dist.csv", True, clustered_analysis)
compute_trip_duration_distribution(dataset, results_folder + "trip_duration_dist.csv", True, clustered_analysis)
compute_trip_distance_by_pickup_hour_distribution(dataset, results_folder + "trip_distance_by_pickup_hour_dist.csv", True, clustered_analysis)
compute_trip_duration_by_pickup_hour_distribution(dataset, results_folder + "trip_duration_by_pickup_hour_dist.csv", True, clustered_analysis)
compute_average_speed_by_pickup_hour_distribution(dataset, results_folder + "avg_speed_by_pickup_hour_dist.csv", True, clustered_analysis)
compute_total_amount_by_pickup_hour_distribution(dataset, results_folder + "total_amount_by_pickup_hour_dist.csv", True, clustered_analysis)
compute_average_total_amount_by_pickup_hour(dataset, results_folder + "avg_total_amount_by_pickup_hour.csv", True, clustered_analysis)
compute_average_duration_by_pickup_location(dataset, results_folder + "avg_duration_by_pickup_location.csv", True, clustered_analysis)
compute_average_distance_by_pickup_location(dataset, results_folder + "avg_distance_by_pickup_location.csv", True, clustered_analysis)
compute_average_speed_by_pickup_location(dataset, results_folder + "avg_speed_by_pickup_location.csv", True, clustered_analysis)
compute_average_total_amount_by_pickup_location(dataset, results_folder + "avg_total_amount_by_pickup_location.csv", True, clustered_analysis)
compute_pickup_location_distribution(dataset, results_folder + "pickup_location_distr.csv", True, clustered_analysis)
compute_pickup_location_by_pickup_hour_distribution(dataset, results_folder + "pickup_location_by_pickup_hour_distr.csv", True, clustered_analysis)
compute_dropoff_location_distribution(dataset, results_folder + "dropoff_location_distr.csv", True, clustered_analysis)
compute_dropoff_location_by_pickup_hour_distribution(dataset, results_folder + "dropoff_location_by_pickup_hour_distr.csv", True, clustered_analysis)
compute_pickup_dropoff_location_by_pickup_hour_distribution(dataset, results_folder + "pickup_dropoff_location_by_pickup_hour_distr.csv", True, clustered_analysis)
compute_passenger_count_distribution(dataset, results_folder + "passenger_count_distr.csv", True, clustered_analysis)
compute_trip_distance_distribution(dataset, results_folder + "trip_distance_distr.csv", True, clustered_analysis)
compute_ratecode_distribution(dataset, results_folder + "ratecode_distr.csv", True, clustered_analysis)
compute_fare_amount_distribution(dataset, results_folder + "fare_amount_distr.csv", True, clustered_analysis)
compute_average_fare_amount_percentage_by_year(dataset, results_folder + "avg_fare_amount_percentage.csv", True, clustered_analysis)
compute_tolls_amount_distribution(dataset, results_folder + "tolls_amount_distr.csv", True, clustered_analysis)
compute_total_amount_distribution(dataset, results_folder + "total_amount_distr.csv", True, clustered_analysis)
compute_profits_by_year(dataset, results_folder + "profits_by_year.csv", True, clustered_analysis)
compute_monthly_profit_percentage_by_year_and_month(dataset, results_folder + "monthly_profit_by_year.csv", True, clustered_analysis)
compute_average_total_amount_by_year(dataset, results_folder + "avg_total_amount_by_year.csv", True, clustered_analysis)
compute_average_total_amount_by_month(dataset, results_folder + "avg_total_amount_by_month.csv", True, clustered_analysis)
compute_mta_tax_profits_by_year(dataset, results_folder + "mta_tax_by_year.csv", True, clustered_analysis)
compute_improvement_surcharge_profits_by_year(dataset, results_folder + "improvement_surcharge_by_year.csv", True, clustered_analysis)
compute_average_extra_by_hour(dataset, results_folder + "avg_extra_by_hour.csv", True, clustered_analysis)
compute_tips_distribution(dataset, results_folder + "tips_distr.csv", True, clustered_analysis)
compute_average_tip_percentage(dataset, results_folder + "avg_tip_percentage.csv", True, clustered_analysis)
compute_average_tip_percentage_by_year(dataset, results_folder + "avg_tip_percentage_by_year.csv", True, clustered_analysis)
compute_payment_type_distribution(dataset, results_folder + "payment_type_distr.csv", True, clustered_analysis)
compute_payment_type_by_year_distribution(dataset, results_folder + "payment_type_distr.csv", True, clustered_analysis)
compute_travels_by_year_and_company(dataset, results_folder + "travels_by_year_and company.csv", True, clustered_analysis)
compute_profits_by_year_and_company(dataset, results_folder + "profits_by_year_and company.csv", True, clustered_analysis)
compute_trip_distance_by_year_and_company_distribution(dataset, results_folder + "trip_distance_by_year_and company_distr.csv", True, clustered_analysis)
compute_passenger_count_by_year_and_company_distribution(dataset, results_folder + "passenger_count_by_year_and_company_distr.csv", True, clustered_analysis)
compute_pickup_location_id_by_company_distribution(dataset, results_folder + "pickup_location_by_company_distr.csv", True, clustered_analysis)

compute_mta_tax_distribution(dataset, results_folder + "mta_tax_distr.csv", True, clustered_analysis)
compute_improvement_surcharge_distribution(dataset, results_folder + "improvement_surcharge_distr.csv", True, clustered_analysis)
compute_extra_distribution(dataset, results_folder + "extra_distr.csv", True, clustered_analysis)
compute_total_amount_distribution(dataset, results_folder + "total_amount_distr.csv", True, clustered_analysis)

compute_airport_distribution(original_dataset, results_folder + "airport_distr.csv", True, clustered_analysis)
compute_total_amount_by_airport(original_dataset, results_folder + "total_amount_by_airport.csv", True, clustered_analysis)