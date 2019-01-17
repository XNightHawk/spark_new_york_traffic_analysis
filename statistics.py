import pyspark
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

import schema_conversion
from schema import *
from computed_columns import *

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