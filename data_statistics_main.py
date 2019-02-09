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

Statistical data analysis of the main dataset

Required files: Clustered dataset

Parameters to set:
master -> The url for the spark cluster, set to local for your convenience
dataset_folder -> Location of the dataset
results_folder -> Location where to save results
clustered_analysis -> Set to True to produce a per cluster analysis instead of the global one
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
compute_pickup_location_id_by_company_distribution(dataset.filter(pyspark.sql.functions.year(dataset[pickup_datetime_property]) >= 2014), results_folder + "pickup_location_by_company_after_2014_distr.csv", True, clustered_analysis)
compute_fare_amount_by_company_distribution(dataset, results_folder + "fare_amount_by_company_distr.csv", True, clustered_analysis)
compute_trip_distance_by_company_distribution(dataset, results_folder + "trip_distance_by_company_distr.csv", True, clustered_analysis)

compute_mta_tax_distribution(dataset, results_folder + "mta_tax_distr.csv", True, clustered_analysis)
compute_improvement_surcharge_distribution(dataset, results_folder + "improvement_surcharge_distr.csv", True, clustered_analysis)
compute_extra_distribution(dataset, results_folder + "extra_distr.csv", True, clustered_analysis)
compute_total_amount_distribution(dataset, results_folder + "total_amount_distr.csv", True, clustered_analysis)

compute_overall_pickups(dataset, results_folder + "overall_pickups.csv", True, clustered_analysis)
compute_overall_dropoffs(dataset, results_folder + "overall_dropoffs.csv", True, clustered_analysis)
compute_overall_fare_amount(dataset, results_folder + "overall_fare_amount.csv", True, clustered_analysis)
compute_overall_total_amount(dataset, results_folder + "overall_total_amount.csv", True, clustered_analysis)

#Computes mean and variance of selected columns
for current_column in [(dataset[payment_type_property], "payment_type"), (dataset[ratecode_id_property], "ratecode_id"), (dayofweek(dataset[dropoff_datetime_property]), 'dayofweek'), (hour(dataset[pickup_datetime_property]), "pickup_hour"), (hour(dataset[dropoff_datetime_property]), "dropoff_hour"), (dataset[passenger_count_property], "passenger_count"), (speed_column(dataset, 'speed'), "speed"), (dataset[trip_distance_property], "distance"), (dataset[fare_amount_property], "fare_amount"), (dataset[tolls_amount_property], 'tolls_amount'), (dataset[tip_amount_property], 'tip_amount')]:

    compute_mean(dataset, results_folder + current_column[1] + "_mean.csv", current_column[0], True, clustered_analysis)
    compute_variance(dataset, results_folder + current_column[1] + "_variance.csv", current_column[0], True, clustered_analysis)

#Traffic movement analysis
for zone in [("manhattan", manhattan_ids), ("bronx", bronx_ids), ("brooklyn", brooklyn_ids), ("queens", queens_ids), ("staten_island", staten_island_ids)]:
    zone_dataset = dataset.filter(dataset[pickup_location_id_property].isin(zone[1]))
    compute_dropoff_location_id_distribution(zone_dataset, results_folder + zone[0] + "_dropoff_location_id_dist.csv", True, clustered_analysis)
    compute_dropoff_location_by_pickup_hour_distribution(zone_dataset, results_folder + zone[0] + "_dropoff_location_id_by_pickup_hour_dist.csv", True, clustered_analysis)


average_column_names = [extra_property, improvement_surcharge_property, tolls_amount_property, fare_amount_property, tip_amount_property]

for current_column_name in average_column_names:

    compute_average_column_by_year(dataset, current_column_name, results_folder + "avg_" + current_column_name + "_by_year.csv", True, clustered_analysis)

# Traffic movement analysis for building network graph
compute_rank_by_pickup_location_and_dropoff_location(dataset, results_folder + "rank_by_pickup_location_and_dropoff_location.csv", True, clustered_analysis)
