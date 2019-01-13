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

def trip_duration_minutes_column(dataset, alias_name = "trip_duration_minutes"):
    return ((unix_timestamp(dataset[dropoff_datetime_property]) / 60).cast(IntegerType()) - (unix_timestamp(dataset[pickup_datetime_property]) / 60).cast(IntegerType())).alias(alias_name)

def speed_column(dataset, alias_name = "average_speed"):
    return (dataset[trip_distance_property] / ((unix_timestamp(dropoff_datetime_property) - unix_timestamp(pickup_datetime_property)) / 3600)).alias(alias_name)

def compute_pickup_hour_distribution(dataset, result_filename, show = False):
    dataset = dataset.select(pyspark.sql.functions.hour(pickup_datetime_property).alias("pickup_hour")).groupBy("pickup_hour").count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()


def compute_pickup_yearday_distribution(dataset, result_filename, show = False):
    dataset = dataset.select(pyspark.sql.functions.dayofyear(pickup_datetime_property).alias("pickup_day")).groupBy("pickup_day").count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_month_distribution(dataset, result_filename, show = False):
    dataset = dataset.select(pyspark.sql.functions.month(pickup_datetime_property).alias("pickup_month")).groupBy("pickup_month").count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_location_id_distribution(dataset, result_filename, show = False):
    dataset = dataset.groupBy(pickup_location_id_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_dropoff_location_id_distribution(dataset, result_filename, show = False):
    dataset = dataset.groupBy(dropoff_location_id_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_passenger_count_distribution(dataset, result_filename, show = False):
    dataset = dataset.groupBy(passenger_count_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

#Computes the trip duration distribution in minutes
def compute_trip_duration_distribution(dataset, result_filename, show = False):
    dataset = dataset.groupBy(trip_duration_minutes_column(dataset)).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_trip_duration_by_pickup_hour_distribution(dataset, result_filename, show = False):
    dataset = dataset.groupBy(trip_duration_minutes_column(dataset), hour(pickup_datetime_property)).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_trip_distance_by_pickup_hour_distribution(dataset, result_filename, show = False):
    dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType()), hour(pickup_datetime_property)).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

#Computes average speed in MPH per pickup hour
def compute_average_speed_by_pickup_hour_distribution(dataset, result_filename, show = False):
    dataset = dataset.select(hour(pickup_datetime_property).alias("pickup_hour"), speed_column(dataset, "average_speed")).groupBy("pickup_hour").avg("average_speed")
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_total_amount_by_pickup_hour_distribution(dataset, result_filename, show = False):
    dataset = dataset.groupBy(hour(pickup_datetime_property), dataset[total_amount_property].cast(IntegerType()).alias("discrete_total_amount")).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_total_amount_by_pickup_hour(dataset, result_filename, show = False):
    dataset = dataset.groupBy(hour(pickup_datetime_property)).avg(total_amount_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_duration_by_pickup_location(dataset, result_filename, show = False):
    dataset = dataset.select(pickup_location_id_property, (unix_timestamp(dropoff_datetime_property) - unix_timestamp(pickup_datetime_property)).alias("duration_seconds")).groupBy(pickup_location_id_property).avg("duration_seconds")
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_distance_by_pickup_location(dataset, result_filename, show = False):
    dataset = dataset.select(pickup_location_id_property, trip_distance_property).groupBy(pickup_location_id_property).avg(trip_distance_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_speed_by_pickup_location(dataset, result_filename, show = False):
    dataset = dataset.select(pickup_location_id_property, speed_column(dataset, "avg_speed")).groupBy(pickup_location_id_property).avg("avg_speed")
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()


def compute_average_total_amount_by_pickup_location(dataset, result_filename, show=False):
    dataset = dataset.select(pickup_location_id_property, total_amount_property).groupBy(pickup_location_id_property).avg(total_amount_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_location_distribution(dataset, result_filename, show=False):
    dataset = dataset.groupBy(pickup_location_id_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_location_by_pickup_hour_distribution(dataset, result_filename, show=False):
    dataset = dataset.groupBy(hour(pickup_datetime_property), pickup_location_id_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_dropoff_location_distribution(dataset, result_filename, show=False):
    dataset = dataset.groupBy(dropoff_location_id_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_dropoff_location_by_pickup_hour_distribution(dataset, result_filename, show=False):
    #Consider dropoff location but divide by pickup hour
    dataset = dataset.groupBy(hour(pickup_datetime_property), dropoff_location_id_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_dropoff_location_by_pickup_hour_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(hour(pickup_datetime_property), pickup_location_id_property, dropoff_location_id_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_passenger_count_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(passenger_count_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_trip_distance_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType())).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()


def compute_ratecode_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(ratecode_id_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()


def compute_fare_amount_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(dataset[fare_amount_property].cast(IntegerType())).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_fare_amount_percentage_by_year(dataset, result_filename, show=False):

    dataset = dataset.select(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), (dataset[fare_amount_property] / dataset[total_amount_property]).alias("fare_amount_percentage")).groupBy("year").avg("fare_amount_percentage")
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_tolls_amount_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(dataset[tolls_amount_property].cast(IntegerType())).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_total_amount_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(dataset[total_amount_property].cast(IntegerType())).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_profits_by_year(dataset, result_filename, show=False):

    dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property])).sum(total_amount_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_monthly_profit_percentage_by_year_and_month(dataset, result_filename, show=False):
    year_profit_dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year")).sum(total_amount_property)
    month_profit_dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), month(dataset[pickup_datetime_property]).alias("month")).sum(total_amount_property)

    dataset = month_profit_dataset.join(year_profit_dataset, "year").select("year", "month", month_profit_dataset["sum(" + total_amount_property + ")"] / year_profit_dataset["sum(" + total_amount_property + ")"])

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_total_amount_by_year(dataset, result_filename, show=False):

    dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property])).avg(total_amount_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_total_amount_by_month(dataset, result_filename, show=False):

    dataset = dataset.groupBy(month(dataset[pickup_datetime_property])).avg(total_amount_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_mta_tax_profits_by_year(dataset, result_filename, show=False):

    dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property])).sum(mta_tax_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_improvement_surcharge_profits_by_year(dataset, result_filename, show=False):

    dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property])).sum(improvement_surcharge_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_extra_by_hour(dataset, result_filename, show=False):

    dataset = dataset.groupBy(hour(pickup_datetime_property).alias("hour")).avg(extra_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_tips_distribution(dataset, result_filename, show=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    dataset = dataset.where(dataset[payment_type_property] == 1).groupBy(dataset[tip_amount_property].cast(IntegerType()).alias("tip_amount")).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_tip_percentage(dataset, result_filename, show=False):

    # Only credit card tips are registered, so it makes sense to consider them only
    dataset = dataset.where(dataset[payment_type_property] == 1).select((dataset[tip_amount_property] / (dataset[total_amount_property] - dataset[tip_amount_property])).alias("tip_percentage")).groupBy().avg("tip_percentage")
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_tip_percentage_by_year(dataset, result_filename, show=False):

    # Only credit card tips are registered, so it makes sense to consider them only
    dataset = dataset.where(dataset[payment_type_property] == 1).select(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), (dataset[tip_amount_property] / (dataset[total_amount_property] - dataset[tip_amount_property])).alias("tip_percentage")).groupBy("year").avg("tip_percentage")
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_payment_type_distribution(dataset, result_filename, show=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    dataset = dataset.groupBy(payment_type_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_payment_type_by_year_distribution(dataset, result_filename, show=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), payment_type_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_travels_by_year_and_company(dataset, result_filename, show=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), taxi_company_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_profits_by_year_and_company(dataset, result_filename, show=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), taxi_company_property).sum(total_amount_property)
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_trip_distance_by_year_and_company_distribution(dataset, result_filename, show=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType()).alias("distance"), taxi_company_property, pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year")).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_passenger_count_by_year_and_company_distribution(dataset, result_filename, show=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    dataset = dataset.groupBy(passenger_count_property, taxi_company_property, pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year")).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_location_id_by_company_distribution(dataset, result_filename, show = False):
    dataset = dataset.groupBy(pickup_location_id_property, taxi_company_property).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def cluster(dataset, max_clusters = 5, clustering_prediction_property = "clustering_predictions"):
    taxi_company_indexed_property = 'taxi_company_indexed'
    taxi_company_indexer = StringIndexer(inputCol=taxi_company_property, outputCol=taxi_company_indexed_property)

    taxi_company_encoded_property = taxi_company_indexed_property + '_encoded'
    ratecode_id_encoded_property = ratecode_id_property + '_encoded'
    payment_type_encoded_property = payment_type_property + '_encoded'

    one_hot_encoder = OneHotEncoderEstimator(inputCols=[taxi_company_indexed_property, ratecode_id_property, payment_type_property], outputCols=[taxi_company_encoded_property, ratecode_id_encoded_property, payment_type_encoded_property])

    passenger_count_scaled_property = passenger_count_property + "_scaled"
    trip_distance_scaled_property = trip_distance_property + "_scaled"
    fare_amount_scaled_property = fare_amount_property + "_scaled"
    tolls_amount_scaled_property = tolls_amount_property + "_scaled"

    #passenger_count_scaler = StandardScaler(inputCol=passenger_count_property, outputCol=passenger_count_scaled_property, withStd=True, withMean=True)
    #trip_distance_scaler = StandardScaler(inputCol=trip_distance_property, outputCol=trip_distance_scaled_property, withStd=True, withMean=True)
    #fare_amount_scaler = StandardScaler(inputCol=fare_amount_property, outputCol=fare_amount_scaled_property, withStd=True, withMean=True)
    #tolls_amount_scaler = StandardScaler(inputCol=tolls_amount_property, outputCol=tolls_amount_scaled_property, withStd=True, withMean=True)

    clustering_features_property = 'features'

    vector_assembler = VectorAssembler(inputCols=[taxi_company_indexed_property, passenger_count_property, trip_distance_property, ratecode_id_encoded_property, fare_amount_property, tolls_amount_property, payment_type_encoded_property], outputCol=clustering_features_property)

    kmeans = KMeans(featuresCol=clustering_features_property, predictionCol=clustering_prediction_property, k=max_clusters)

    #pipeline = Pipeline(stages=[taxi_company_indexer, one_hot_encoder, passenger_count_scaler, trip_distance_scaler, fare_amount_scaler, tolls_amount_scaler, vector_assembler, kmeans])
    pipeline = Pipeline(stages=[taxi_company_indexer, one_hot_encoder, vector_assembler, kmeans])

    model = pipeline.fit(dataset)

    return model

def compute_mta_tax_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(dataset[mta_tax_property].cast(IntegerType()).alias("mta_tax")).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_improvement_surcharge_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(dataset[improvement_surcharge_property].cast(IntegerType()).alias("improvement_surcharge")).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_extra_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(
        dataset[extra_property].cast(IntegerType()).alias("extra")).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_total_amount_distribution(dataset, result_filename, show=False):

    dataset = dataset.groupBy(dataset[total_amount_property].cast(IntegerType())).count()
    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

appName = 'Parquet Converter'
master = 'local[*]'

sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(appName).getOrCreate()


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

    current_dataset = spark.read.parquet('file://' + dataset_folder + archive + '_common.parquet')
    if dataset is None:
        dataset = current_dataset
    else:
        dataset = dataset.union(current_dataset)


#dataset.printSchema()
#dataset.show()

#compute_pickup_hour_distribution(dataset, results_folder + "pickup_hour_dist.csv", True)
#compute_pickup_yearday_distribution(dataset, results_folder + "pickup_yearday_dist.csv", True)
#compute_pickup_month_distribution(dataset, results_folder + "pickup_month_dist.csv", True)
#compute_pickup_location_id_distribution(dataset, results_folder + "pickup_location_id_dist.csv", True)
#compute_dropoff_location_id_distribution(dataset, results_folder + "dropoff_location_id_dist.csv", True)
#compute_passenger_count_distribution(dataset, results_folder + "passenger_count_dist.csv", True)
#compute_trip_duration_distribution(dataset, results_folder + "trip_duration_dist.csv", True)
#compute_trip_distance_by_pickup_hour_distribution(dataset, results_folder + "trip_distance_by_pickup_hour_dist.csv", True)
#compute_trip_duration_by_pickup_hour_distribution(dataset, results_folder + "trip_duration_by_pickup_hour_dist.csv", True)
#compute_average_speed_by_pickup_hour_distribution(dataset, results_folder + "avg_speed_by_pickup_hour_dist.csv", True)
#compute_total_amount_by_pickup_hour_distribution(dataset, results_folder + "total_amount_by_pickup_hour_dist.csv", True)
#compute_average_total_amount_by_pickup_hour(dataset, results_folder + "avg_total_amount_by_pickup_hour.csv", True)
#compute_average_duration_by_pickup_location(dataset, results_folder + "avg_duration_by_pickup_location.csv", True)
#compute_average_distance_by_pickup_location(dataset, results_folder + "avg_distance_by_pickup_location.csv", True)
#compute_average_speed_by_pickup_location(dataset, results_folder + "avg_speed_by_pickup_location.csv", True)
#compute_average_total_amount_by_pickup_location(dataset, results_folder + "avg_total_amount_by_pickup_location.csv", True)
#compute_pickup_location_distribution(dataset, results_folder + "pickup_location_distr.csv", True)
#compute_pickup_location_by_pickup_hour_distribution(dataset, results_folder + "pickup_location_by_pickup_hour_distr.csv", True)
#compute_dropoff_location_distribution(dataset, results_folder + "dropoff_location_distr.csv", True)
#compute_dropoff_location_by_pickup_hour_distribution(dataset, results_folder + "dropoff_location_by_pickup_hour_distr.csv", True)
#compute_pickup_dropoff_location_by_pickup_hour_distribution(dataset, results_folder + "pickup_dropoff_location_by_pickup_hour_distr.csv", True)
#compute_passenger_count_distribution(dataset, results_folder + "passenger_count_distr.csv", True)
#compute_trip_distance_distribution(dataset, results_folder + "trip_distance_distr.csv", True)
#compute_ratecode_distribution(dataset, results_folder + "ratecode_distr.csv", True)
#compute_fare_amount_distribution(dataset, results_folder + "fare_amount_distr.csv", True)
#compute_average_fare_amount_percentage_by_year(dataset, results_folder + "avg_fare_amount_percentage.csv", True)
#compute_tolls_amount_distribution(dataset, results_folder + "tolls_amount_distr.csv", True)
#compute_total_amount_distribution(dataset, results_folder + "total_amount_distr.csv", True)
#compute_profits_by_year(dataset, results_folder + "profits_by_year.csv", True)
#compute_monthly_profit_percentage_by_year_and_month(dataset, results_folder + "monthly_profit_by_year.csv", True)
#compute_average_total_amount_by_year(dataset, results_folder + "avg_total_amount_by_year.csv", True)
#compute_average_total_amount_by_month(dataset, results_folder + "avg_total_amount_by_month.csv", True)
#compute_mta_tax_profits_by_year(dataset, results_folder + "mta_tax_by_year.csv", True)
#compute_improvement_surcharge_profits_by_year(dataset, results_folder + "improvement_surcharge_by_year.csv", True)
#compute_average_extra_by_hour(dataset, results_folder + "avg_extra_by_hour.csv", True)
#compute_tips_distribution(dataset, results_folder + "tips_distr.csv", True)
#compute_average_tip_percentage(dataset, results_folder + "avg_tip_percentage.csv", True)
#compute_average_tip_percentage_by_year(dataset, results_folder + "avg_tip_percentage_by_year.csv", True)
#compute_payment_type_distribution(dataset, results_folder + "payment_type_distr.csv", True)
#compute_payment_type_by_year_distribution(dataset, results_folder + "payment_type_distr.csv", True)

#compute_travels_by_year_and_company(dataset, results_folder + "travels_by_year_and company.csv", True)
#compute_profits_by_year_and_company(dataset, results_folder + "profits_by_year_and company.csv", True)
#compute_trip_distance_by_year_and_company_distribution(dataset, results_folder + "trip_distance_by_year_and company_distr.csv", True)
#compute_passenger_count_by_year_and_company_distribution(dataset, results_folder + "passenger_count_by_year_and_company_distr.csv", True)
#compute_pickup_location_id_by_company_distribution(dataset, results_folder + "pickup_location_by_company_distr.csv", True)


#compute_mta_tax_distribution(dataset, results_folder + "mta_tax_distr.csv", True)
#compute_improvement_surcharge_distribution(dataset, results_folder + "improvement_surcharge_distr.csv", True)
#compute_extra_distribution(dataset, results_folder + "extra_distr.csv", True)
compute_total_amount_distribution(dataset, results_folder + "total_amount_distr.csv", True)

'''
#dataset = dataset.sample(0.0001)
dataset = dataset.dropna()
print("Building clustering Model")
clustering_model = cluster(dataset)
print("Performing clustering")
clustered_dataset = clustering_model.transform(dataset)
print("Clustering done")
clustered_dataset.show(1000)
'''