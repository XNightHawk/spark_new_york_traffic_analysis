#
#    ________    ______   ______     _          ____        __
#   /_  __/ /   / ____/  /_  __/____(_)___     / __ \____ _/ /_____ _
#    / / / /   / /        / / / ___/ / __ \   / / / / __ `/ __/ __ `/
#   / / / /___/ /___     / / / /  / / /_/ /  / /_/ / /_/ / /_/ /_/ /
#  /_/ /_____/\____/    /_/ /_/  /_/ .___/  /_____/\__,_/\__/\__,_/
#                               /_/
#
#
#  Authors: Willi Menapace <willi.menapace@studenti.unitn.it>
#           Luca Zanells <luca.zanella-3@studenti.unitn.it>
#
#  Produces plots for the main data analysis
#

library(ggplot2)
library(RColorBrewer)
library(rgdal)
library(ggmap)

source("plotting_library.R")

# First read in the shapefile, using the path to the shapefile and the shapefile name minus the
# extension as arguments
shapefile <- readOGR("shapefile", "taxi_zones")
# Next the shapefile has to be converted to a dataframe for use in ggplot2
shapefile_df <- fortify(shapefile)
shapefile_df$id = as.numeric(shapefile_df$id)
shapefile_df$id = shapefile_df$id + 1

plots_location = 'plots\\'
dataset_location = 'Z:\\dataset\\stats\\'
source("standard_dataset.R")


plot_avg_distance_by_pickup_location(avg_distance_by_pickup_location, shapefile_df, paste(plots_location, 'avg_distance_by_pickup_location', sep=''))
plot_avg_duration_by_pickup_location(avg_duration_by_pickup_location, paste(plots_location, 'avg_duration_by_pickup_location', sep=''))
plot_avg_extra_by_hour(avg_extra_by_hour, paste(plots_location, 'avg_extra_by_hour', sep=''))
plot_avg_fare_amount_percentage(avg_fare_amount_percentage, paste(plots_location, 'avg_fare_amount_percentage', sep=''))
plot_avg_speed_by_pickup_hour_dist(avg_speed_by_pickup_hour_dist, paste(plots_location, 'avg_speed_by_pickup_hour_dist', sep=''))
plot_avg_speed_by_pickup_location(avg_speed_by_pickup_location, shapefile_df, paste(plots_location, 'avg_speed_by_pickup_location', sep=''))
plot_avg_tip_percentage(avg_tip_percentage, paste(plots_location, 'avg_tip_percentage', sep=''))
plot_avg_tip_percentage_by_year(avg_tip_percentage_by_year, paste(plots_location, 'avg_tip_percentage_by_year', sep=''))
plot_avg_total_amount_by_month(avg_total_amount_by_month, paste(plots_location, 'avg_total_amount_by_month', sep=''))
plot_avg_total_amount_by_pickup_hour(avg_total_amount_by_pickup_hour, paste(plots_location, 'avg_total_amount_by_pickup_hour', sep=''))
plot_avg_total_amount_by_pickup_location(avg_total_amount_by_pickup_location, shapefile_df, paste(plots_location, 'avg_total_amount_by_pickup_location', sep=''))
plot_avg_total_amount_by_year(avg_total_amount_by_year, paste(plots_location, 'avg_total_amount_by_year', sep=''))
plot_dropoff_location_by_pickup_hour_distr(dropoff_location_by_pickup_hour_distr, paste(plots_location, 'dropoff_location_by_pickup_hour_distr', sep=''))
plot_dropoff_location_id_dist(dropoff_location_id_dist, shapefile_df, paste(plots_location, 'dropoff_location_id_dist', sep=''))
plot_extra_distr(extra_distr, paste(plots_location, 'extra_distr', sep=''))
plot_fare_amount_by_company_distr(fare_amount_by_company_distr, paste(plots_location, 'fare_amount_by_company_distr', sep=''))
plot_fare_amount_distr(fare_amount_distr, paste(plots_location, 'fare_amount_distr', sep=''))
plot_fhv_vs_taxi(fhv_vs_taxi, paste(plots_location, 'fhv_vs_taxi', sep=''))
plot_improvement_surcharge_by_year(improvement_surcharge_by_year, paste(plots_location, 'improvement_surcharge_by_year', sep=''))
plot_improvement_surcharge_distr(improvement_surcharge_distr, paste(plots_location, 'improvement_surcharge_distr', sep=''))
plot_monthly_profit_by_year(monthly_profit_by_year, paste(plots_location, 'monthly_profit_by_year', sep=''))
plot_mta_tax_by_year(mta_tax_by_year, paste(plots_location, 'mta_tax_by_year', sep=''))
plot_mta_tax_distr(mta_tax_distr, paste(plots_location, 'mta_tax_distr', sep=''))
plot_overall_pickups(overall_pickups, paste(plots_location, 'overall_pickups', sep=''))
plot_overall_dropoffs(overall_dropoffs, paste(plots_location, 'overall_dropoffs', sep=''))
plot_overall_fare_amount(overall_fare_amount, paste(plots_location, 'overall_fare_amount', sep=''))
plot_overall_total_amount(overall_total_amount, paste(plots_location, 'overall_total_amount', sep=''))
plot_passenger_count_by_year_and_company_distr(passenger_count_by_year_and_company_distr, paste(plots_location, 'passenger_count_by_year_and_company_distr', sep=''))
plot_passenger_count_dist(passenger_count_dist, paste(plots_location, 'passenger_count_dist', sep=''))
plot_payment_type_distr(payment_type_distr, paste(plots_location, 'payment_type_distr', sep=''))
plot_pickup_dropoff_location_by_pickup_hour_distr(pickup_dropoff_location_by_pickup_hour_distr, paste(plots_location, 'pickup_dropoff_location_by_pickup_hour_distr', sep=''))
plot_pickup_hour_dist(pickup_hour_dist, paste(plots_location, 'pickup_hour_dist', sep=''))
plot_pickup_location_by_company_distr(pickup_location_by_company_after_2014_distr, paste(plots_location, 'pickup_location_by_company_after_2014_distr', sep=''))
plot_pickup_location_by_pickup_hour_distr(pickup_location_by_pickup_hour_distr, paste(plots_location, 'pickup_location_by_pickup_hour_distr', sep=''))
plot_pickup_location_id_dist(pickup_location_id_dist, shapefile_df, paste(plots_location, 'pickup_location_id_dist', sep=''))
plot_pickup_month_dist(pickup_month_dist, paste(plots_location, 'pickup_month_dist', sep=''))
plot_pickup_yearday_dist(pickup_yearday_dist, paste(plots_location, 'pickup_yearday_dist', sep=''))
plot_profits_by_year(profits_by_year, paste(plots_location, 'profits_by_year', sep=''))
plot_profits_by_year_and_company(profits_by_year_and_company, paste(plots_location, 'profits_by_year_and_company', sep=''))
plot_ratecode_distr(ratecode_distr, paste(plots_location, 'ratecode_distr', sep=''))
plot_tips_distr(tips_distr, paste(plots_location, 'tips_distr', sep=''))
plot_tolls_amount_distr(tolls_amount_distr, paste(plots_location, 'tolls_amount_distr', sep=''))
plot_total_amount_distr(total_amount_distr, paste(plots_location, 'total_amount_distr', sep=''))
plot_travels_by_year(travels_by_year, paste(plots_location, 'travels_by_year', sep=''))
plot_travels_by_year_and_company(travels_by_year_and_company, paste(plots_location, 'travels_by_year_and_company', sep=''))
plot_trip_distance_by_company_distr(trip_distance_by_company_distr, paste(plots_location, 'trip_distance_by_company_distr', sep=''))
plot_trip_distance_by_pickup_hour_dist(trip_distance_by_pickup_hour_dist, paste(plots_location, 'trip_distance_by_pickup_hour_dist', sep=''))
plot_trip_distance_by_year_and_company_distr(trip_distance_by_year_and_company_distr, paste(plots_location, 'trip_distance_by_year_and_company_distr', sep=''))
plot_trip_distance_distr(trip_distance_distr, paste(plots_location, 'trip_distance_distr', sep=''))
plot_trip_duration_by_pickup_hour_dist(trip_duration_by_pickup_hour_dist, paste(plots_location, 'trip_duration_by_pickup_hour_dist', sep=''))
plot_trip_duration_dist(trip_duration_dist, paste(plots_location, 'trip_duration_dist', sep=''))

plot_dropoff_location_by_pickup_hour_distr(bronx_dropoff_location_id_by_pickup_hour_dist, paste(plots_location, 'bronx_dropoff_location_id_by_pickup_hour_dist', sep=''))
plot_dropoff_location_by_pickup_hour_distr(brooklyn_dropoff_location_id_by_pickup_hour_dist, paste(plots_location, 'brooklyn_dropoff_location_id_by_pickup_hour_dist', sep=''))
plot_dropoff_location_by_pickup_hour_distr(manhattan_dropoff_location_id_by_pickup_hour_dist, paste(plots_location, 'manhattan_dropoff_location_id_by_pickup_hour_dist', sep=''))
plot_dropoff_location_by_pickup_hour_distr(queens_dropoff_location_id_by_pickup_hour_dist, paste(plots_location, 'queens_dropoff_location_id_by_pickup_hour_dist', sep=''))
plot_dropoff_location_by_pickup_hour_distr(staten_island_dropoff_location_id_by_pickup_hour_dist, paste(plots_location, 'staten_island_dropoff_location_id_by_pickup_hour_dist', sep=''))

plot_dropoff_location_id_dist(bronx_dropoff_location_id_dist, shapefile_df, paste(plots_location, 'bronx_dropoff_location_id_dist', sep=''))
plot_dropoff_location_id_dist(brooklyn_dropoff_location_id_dist, shapefile_df, paste(plots_location, 'brooklyn_dropoff_location_id_dist', sep=''))
plot_dropoff_location_id_dist(manhattan_dropoff_location_id_dist, shapefile_df, paste(plots_location, 'manhattan_dropoff_location_id_dist', sep=''))
plot_dropoff_location_id_dist(queens_dropoff_location_id_dist, shapefile_df, paste(plots_location, 'queens_dropoff_location_id_dist', sep=''))
plot_dropoff_location_id_dist(staten_island_dropoff_location_id_dist, shapefile_df, paste(plots_location, 'staten_island_dropoff_location_id_dist', sep=''))









