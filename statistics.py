import pyspark
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit

import schema_conversion
from schema import *
from computed_columns import *

def compute_pickup_hour_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.select(pyspark.sql.functions.hour(pickup_datetime_property).alias("pickup_hour")).groupBy("pickup_hour").count()
    else:
        dataset = dataset.select(pyspark.sql.functions.hour(pickup_datetime_property).alias("pickup_hour"), clustering_class_property).groupBy("pickup_hour", clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()


def compute_pickup_yearday_distribution(dataset, result_filename, show=False, separe_clusters=False):
    if separe_clusters == False:
        dataset = dataset.select(pyspark.sql.functions.dayofyear(pickup_datetime_property).alias("pickup_day")).groupBy("pickup_day").count()
    else:
        dataset = dataset.select(pyspark.sql.functions.dayofyear(pickup_datetime_property).alias("pickup_day"), clustering_class_property).groupBy("pickup_day", clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()


def compute_pickup_weekday_distribution(dataset, result_filename, show=False, separe_clusters=False):
    if separe_clusters == False:
        dataset = dataset.select(pyspark.sql.functions.dayofweek(pickup_datetime_property).alias("pickup_day")).groupBy("pickup_day").count()
    else:
        dataset = dataset.select(pyspark.sql.functions.dayofweek(pickup_datetime_property).alias("pickup_day"), clustering_class_property).groupBy("pickup_day", clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_month_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.select(pyspark.sql.functions.month(pickup_datetime_property).alias("pickup_month")).groupBy("pickup_month").count()
    else:
        dataset = dataset.select(pyspark.sql.functions.month(pickup_datetime_property).alias("pickup_month"), clustering_class_property).groupBy("pickup_month", clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_location_id_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(pickup_location_id_property).count()
    else:
        dataset = dataset.groupBy(pickup_location_id_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_dropoff_location_id_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dropoff_location_id_property).count()
    else:
        dataset = dataset.groupBy(dropoff_location_id_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_passenger_count_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(passenger_count_property).count()
    else:
        dataset = dataset.groupBy(passenger_count_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

#Computes the trip duration distribution in minutes
def compute_trip_duration_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(trip_duration_minutes_column(dataset)).count()
    else:
        dataset = dataset.groupBy(trip_duration_minutes_column(dataset), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_trip_duration_by_pickup_hour_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(trip_duration_minutes_column(dataset), hour(pickup_datetime_property)).count()
    else:
        dataset = dataset.groupBy(trip_duration_minutes_column(dataset), hour(pickup_datetime_property), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_trip_distance_by_pickup_hour_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType()), hour(pickup_datetime_property)).count()
    else:
        dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType()), hour(pickup_datetime_property), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

#Computes average speed in MPH per pickup hour
def compute_average_speed_by_pickup_hour_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.select(hour(pickup_datetime_property).alias("pickup_hour"), speed_column(dataset, "average_speed")).groupBy("pickup_hour").avg("average_speed")
    else:
        dataset = dataset.select(hour(pickup_datetime_property).alias("pickup_hour"), speed_column(dataset, "average_speed"), clustering_class_property).groupBy("pickup_hour", clustering_class_property).avg("average_speed")

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_total_amount_by_pickup_hour_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(hour(pickup_datetime_property), dataset[total_amount_property].cast(IntegerType()).alias("discrete_total_amount")).count()
    else:
        dataset = dataset.groupBy(hour(pickup_datetime_property), dataset[total_amount_property].cast(IntegerType()).alias("discrete_total_amount"), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_total_amount_by_pickup_hour(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(hour(pickup_datetime_property)).avg(total_amount_property)
    else:
        dataset = dataset.groupBy(hour(pickup_datetime_property), clustering_class_property).avg(total_amount_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_duration_by_pickup_location(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.select(pickup_location_id_property, (unix_timestamp(dropoff_datetime_property) - unix_timestamp(pickup_datetime_property)).alias("duration_seconds")).groupBy(pickup_location_id_property).avg("duration_seconds")
    else:
        dataset = dataset.select(pickup_location_id_property, (unix_timestamp(dropoff_datetime_property) - unix_timestamp(pickup_datetime_property)).alias("duration_seconds"), clustering_class_property).groupBy(pickup_location_id_property, clustering_class_property).avg("duration_seconds")

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_distance_by_pickup_location(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.select(pickup_location_id_property, trip_distance_property).groupBy(pickup_location_id_property).avg(trip_distance_property)
    else:
        dataset = dataset.select(pickup_location_id_property, trip_distance_property, clustering_class_property).groupBy(pickup_location_id_property, clustering_class_property).avg(trip_distance_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_speed_by_pickup_location(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.select(pickup_location_id_property, speed_column(dataset, "avg_speed")).groupBy(pickup_location_id_property).avg("avg_speed")
    else:
        dataset = dataset.select(pickup_location_id_property, speed_column(dataset, "avg_speed"), clustering_class_property).groupBy(pickup_location_id_property, clustering_class_property).avg("avg_speed")

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()


def compute_average_total_amount_by_pickup_location(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.select(pickup_location_id_property, total_amount_property).groupBy(pickup_location_id_property).avg(total_amount_property)
    else:
        dataset = dataset.select(pickup_location_id_property, total_amount_property, clustering_class_property).groupBy(pickup_location_id_property, clustering_class_property).avg(total_amount_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_location_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(pickup_location_id_property).count()
    else:
        dataset = dataset.groupBy(pickup_location_id_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_location_by_pickup_hour_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(hour(pickup_datetime_property), pickup_location_id_property).count()
    else:
        dataset = dataset.groupBy(hour(pickup_datetime_property), pickup_location_id_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_dropoff_location_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dropoff_location_id_property).count()
    else:
        dataset = dataset.groupBy(dropoff_location_id_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_dropoff_location_by_pickup_hour_distribution(dataset, result_filename, show=False, separe_clusters=False):

    #Consider dropoff location but divide by pickup hour
    if separe_clusters == False:
        dataset = dataset.groupBy(hour(pickup_datetime_property), dropoff_location_id_property).count()
    else:
        dataset = dataset.groupBy(hour(pickup_datetime_property), dropoff_location_id_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_dropoff_location_by_pickup_hour_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(hour(pickup_datetime_property), pickup_location_id_property, dropoff_location_id_property).count()
    else:
        dataset = dataset.groupBy(hour(pickup_datetime_property), pickup_location_id_property, dropoff_location_id_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_passenger_count_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(passenger_count_property).count()
    else:
        dataset = dataset.groupBy(passenger_count_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_trip_distance_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType())).count()
    else:
        dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType()), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()


def compute_ratecode_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(ratecode_id_property).count()
    else:
        dataset = dataset.groupBy(ratecode_id_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()


def compute_fare_amount_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[fare_amount_property].cast(IntegerType())).count()
    else:
        dataset = dataset.groupBy(dataset[fare_amount_property].cast(IntegerType()), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_fare_amount_percentage_by_year(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.select(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), (dataset[fare_amount_property] / dataset[total_amount_property]).alias("fare_amount_percentage")).groupBy("year").avg("fare_amount_percentage")
    else:
        dataset = dataset.select(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), (dataset[fare_amount_property] / dataset[total_amount_property]).alias("fare_amount_percentage"), clustering_class_property).groupBy("year", clustering_class_property).avg("fare_amount_percentage")

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_tolls_amount_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[tolls_amount_property].cast(IntegerType())).count()
    else:
        dataset = dataset.groupBy(dataset[tolls_amount_property].cast(IntegerType()), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_total_amount_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[total_amount_property].cast(IntegerType())).count()
    else:
        dataset = dataset.groupBy(dataset[total_amount_property].cast(IntegerType()), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_profits_by_year(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property])).sum(total_amount_property)
    else:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]), clustering_class_property).sum(total_amount_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_monthly_profit_percentage_by_year_and_month(dataset, result_filename, show=False, separe_clusters=False):


    if separe_clusters == False:
        year_profit_dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year")).sum(total_amount_property)
        month_profit_dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), month(dataset[pickup_datetime_property]).alias("month")).sum(total_amount_property)

        dataset = month_profit_dataset.join(year_profit_dataset, "year").select("year", "month", month_profit_dataset["sum(" + total_amount_property + ")"] / year_profit_dataset["sum(" + total_amount_property + ")"])
    else:
        year_profit_dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), clustering_class_property).sum(total_amount_property)
        month_profit_dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), month(dataset[pickup_datetime_property]).alias("month"), clustering_class_property).sum(total_amount_property)

        dataset = month_profit_dataset.join(year_profit_dataset, ["year", clustering_class_property]).select("year", "month", clustering_class_property, month_profit_dataset["sum(" + total_amount_property + ")"] / year_profit_dataset["sum(" + total_amount_property + ")"])

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_total_amount_by_year(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property])).avg(total_amount_property)
    else:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]), clustering_class_property).avg(total_amount_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_total_amount_by_month(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(month(dataset[pickup_datetime_property])).avg(total_amount_property)
    else:
        dataset = dataset.groupBy(month(dataset[pickup_datetime_property]), clustering_class_property).avg(total_amount_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_mta_tax_profits_by_year(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property])).sum(mta_tax_property)
    else:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]), clustering_class_property).sum(mta_tax_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_improvement_surcharge_profits_by_year(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property])).sum(improvement_surcharge_property)
    else:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]), clustering_class_property).sum(improvement_surcharge_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_extra_by_hour(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(hour(pickup_datetime_property).alias("hour")).avg(extra_property)
    else:
        dataset = dataset.groupBy(hour(pickup_datetime_property).alias("hour"), clustering_class_property).avg(extra_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_tips_distribution(dataset, result_filename, show=False, separe_clusters=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.where(dataset[payment_type_property] == 1).groupBy(dataset[tip_amount_property].cast(IntegerType()).alias("tip_amount")).count()
    else:
        dataset = dataset.where(dataset[payment_type_property] == 1).groupBy(dataset[tip_amount_property].cast(IntegerType()).alias("tip_amount"), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_tip_percentage(dataset, result_filename, show=False, separe_clusters=False):

    # Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.where(dataset[payment_type_property] == 1).select((dataset[tip_amount_property] / (dataset[total_amount_property] - dataset[tip_amount_property])).alias("tip_percentage")).groupBy().avg("tip_percentage")
    else:
        dataset = dataset.where(dataset[payment_type_property] == 1).select((dataset[tip_amount_property] / (dataset[total_amount_property] - dataset[tip_amount_property])).alias("tip_percentage"), clustering_class_property).groupBy(clustering_class_property).avg("tip_percentage")

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_average_tip_percentage_by_year(dataset, result_filename, show=False, separe_clusters=False):

    # Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.where(dataset[payment_type_property] == 1).select(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), (dataset[tip_amount_property] / (dataset[total_amount_property] - dataset[tip_amount_property])).alias("tip_percentage")).groupBy("year").avg("tip_percentage")
    else:
        dataset = dataset.where(dataset[payment_type_property] == 1).select(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), (dataset[tip_amount_property] / (dataset[total_amount_property] - dataset[tip_amount_property])).alias("tip_percentage"), clustering_class_property).groupBy("year", clustering_class_property).avg("tip_percentage")

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_payment_type_distribution(dataset, result_filename, show=False, separe_clusters=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.groupBy(payment_type_property).count()
    else:
        dataset = dataset.groupBy(payment_type_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_payment_type_by_year_distribution(dataset, result_filename, show=False, separe_clusters=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), payment_type_property).count()
    else:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), payment_type_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_travels_by_year_and_company(dataset, result_filename, show=False, separe_clusters=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), taxi_company_property).count()
    else:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), taxi_company_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_profits_by_year_and_company(dataset, result_filename, show=False, separe_clusters=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), taxi_company_property).sum(total_amount_property)
    else:
        dataset = dataset.groupBy(pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), taxi_company_property, clustering_class_property).sum(total_amount_property)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_trip_distance_by_year_and_company_distribution(dataset, result_filename, show=False, separe_clusters=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType()).alias("distance"), taxi_company_property, pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year")).count()
    else:
        dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType()).alias("distance"), taxi_company_property, pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_passenger_count_by_year_and_company_distribution(dataset, result_filename, show=False, separe_clusters=False):

    #Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.groupBy(passenger_count_property, taxi_company_property, pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year")).count()
    else:
        dataset = dataset.groupBy(passenger_count_property, taxi_company_property, pyspark.sql.functions.year(dataset[pickup_datetime_property]).alias("year"), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_pickup_location_id_by_company_distribution(dataset, result_filename, show = False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(pickup_location_id_property, taxi_company_property).count()
    else:
        dataset = dataset.groupBy(pickup_location_id_property, taxi_company_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_mta_tax_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[mta_tax_property].cast(IntegerType()).alias("mta_tax")).count()
    else:
        dataset = dataset.groupBy(dataset[mta_tax_property].cast(IntegerType()).alias("mta_tax"), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_improvement_surcharge_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[improvement_surcharge_property].cast(IntegerType()).alias("improvement_surcharge")).count()
    else:
        dataset = dataset.groupBy(dataset[improvement_surcharge_property].cast(IntegerType()).alias("improvement_surcharge"), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_extra_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[extra_property].cast(IntegerType()).alias("extra")).count()
    else:
        dataset = dataset.groupBy(dataset[extra_property].cast(IntegerType()).alias("extra"), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_total_amount_distribution(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[total_amount_property].cast(IntegerType())).count()
    else:
        dataset = dataset.groupBy(dataset[total_amount_property].cast(IntegerType()), clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_mean(dataset, result_filename, column, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy()
    else:
        dataset = dataset.groupBy(clustering_class_property)

    dataset = dataset.agg(mean(column))

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_variance(dataset, result_filename, column, show=False, separe_clusters=False):

    if separe_clusters == False:
        grouped_dataset = dataset.groupBy()
    else:
        grouped_dataset = dataset.groupBy(clustering_class_property)

    #Var[X] = E[X^2] - E[X]^2
    dataset = grouped_dataset.agg(mean(pow(column, 2)) - pow(mean(column), 2))

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_airport_distribution(dataset, result_filename, show=False, separe_clusters=False):
    airport_locations = [132, 138]
    airport_dataset = dataset.filter(dataset[dropoff_location_id_property].isin(airport_locations) | dataset[pickup_location_id_property].isin(airport_locations) | (dataset[ratecode_id_property] == 3))

    non_airport_dataset = dataset.filter((dataset[dropoff_location_id_property].isin(airport_locations) | dataset[pickup_location_id_property].isin(airport_locations) | (dataset[ratecode_id_property] == 3)) == False)

    jfk_dataset = airport_dataset.filter((dataset[dropoff_location_id_property] == 132) | (dataset[pickup_location_id_property] == 132))
    laguardia_dataset = airport_dataset.filter((dataset[dropoff_location_id_property] == 138) | (dataset[pickup_location_id_property] == 138))
    newark_dataset = airport_dataset.filter(dataset[ratecode_id_property] == 3)

    non_airport_dataset = non_airport_dataset.groupBy().count()
    jfk_dataset = jfk_dataset.groupBy().count()
    laguardia_dataset = laguardia_dataset.groupBy().count()
    newark_dataset = newark_dataset.groupBy().count()

    non_airport_dataset = non_airport_dataset.withColumn("class", lit("no_airport"))
    jfk_dataset = jfk_dataset.withColumn("class", lit("jfk"))
    laguardia_dataset = laguardia_dataset.withColumn("class", lit("laguardia"))
    newark_dataset = newark_dataset.withColumn("class", lit("newark"))

    dataset = non_airport_dataset.union(jfk_dataset).union(laguardia_dataset).union(newark_dataset)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_total_amount_by_airport(dataset, result_filename, show=False, separe_clusters=False):
    airport_locations = [132, 138]
    airport_dataset = dataset.filter(dataset[dropoff_location_id_property].isin(airport_locations) | dataset[
        pickup_location_id_property].isin(airport_locations) | (dataset[ratecode_id_property] == 3))

    non_airport_dataset = dataset.filter((dataset[dropoff_location_id_property].isin(airport_locations) |
                                          dataset[pickup_location_id_property].isin(airport_locations) | (
                                                      dataset[ratecode_id_property] == 3)) == False)

    jfk_dataset = airport_dataset.filter(
        (dataset[dropoff_location_id_property] == 132) | (dataset[pickup_location_id_property] == 132))
    laguardia_dataset = airport_dataset.filter(
        (dataset[dropoff_location_id_property] == 138) | (dataset[pickup_location_id_property] == 138))
    newark_dataset = airport_dataset.filter(dataset[ratecode_id_property] == 3)

    non_airport_dataset = non_airport_dataset.groupBy().sum(total_amount_property)
    jfk_dataset = jfk_dataset.groupBy().sum(total_amount_property)
    laguardia_dataset = laguardia_dataset.groupBy().sum(total_amount_property)
    newark_dataset = newark_dataset.groupBy().sum(total_amount_property)

    non_airport_dataset = non_airport_dataset.withColumn("class", lit("no_airport"))
    jfk_dataset = jfk_dataset.withColumn("class", lit("jfk"))
    laguardia_dataset = laguardia_dataset.withColumn("class", lit("laguardia"))
    newark_dataset = newark_dataset.withColumn("class", lit("newark"))

    dataset = non_airport_dataset.union(jfk_dataset).union(laguardia_dataset).union(newark_dataset)

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_fare_amount_by_company_distribution(dataset, result_filename, show=False, separe_clusters=False):

    # Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[fare_amount_property].cast(IntegerType()).alias("fare_amount"),
                                  taxi_company_property).count()
    else:
        dataset = dataset.groupBy(dataset[fare_amount_property].cast(IntegerType()).alias("fare_amount"),
                                  taxi_company_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_trip_distance_by_company_distribution(dataset, result_filename, show=False, separe_clusters=False):

    # Only credit card tips are registered, so it makes sense to consider them only
    if separe_clusters == False:
        dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType()).alias("distance"),
                                  taxi_company_property).count()
    else:
        dataset = dataset.groupBy(dataset[trip_distance_property].cast(IntegerType()).alias("distance"),
                                  taxi_company_property, clustering_class_property).count()

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()

def compute_rank_by_pickup_location_and_dropoff_location(dataset, result_filename, show=False, separe_clusters=False):

    if separe_clusters == False:
        dataset = dataset.groupBy(pickup_location_id_property, dropoff_location_id_property).count()
    else:
        dataset = dataset.groupBy(pickup_location_id_property, dropoff_location_id_property,
                                  clustering_class_property).count()

    window = Window.partitionBy(dataset[pickup_location_id_property]).orderBy(dataset['count'].desc())
    dataset = dataset.select('*', rank().over(window).alias('rank'))

    if show:
        dataset.cache()

    dataset.toPandas().to_csv(result_filename, header=True)

    if show:
        dataset.show()
        dataset.unpersist()