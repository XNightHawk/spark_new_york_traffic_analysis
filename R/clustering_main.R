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
#  Performs clustered data specific analysis
#

library(ggplot2)
library(RColorBrewer)
library(rgdal)
library(ggmap)

source("clustered_plotting_library.R")

# First read in the shapefile, using the path to the shapefile and the shapefile name minus the
# extension as arguments
shapefile <- readOGR("shapefile", "taxi_zones")
# Next the shapefile has to be converted to a dataframe for use in ggplot2
shapefile_df <- fortify(shapefile)
shapefile_df$id = as.numeric(shapefile_df$id)
shapefile_df$id = shapefile_df$id + 1

plots_location = 'clustered_plots\\'
dataset_location = 'Z:\\dataset\\clustered_stats\\'
source("clustered_dataset.R")

plot_clulstered_trip_distance_distr(trip_distance_distr, paste(plots_location, 'trip_distance_distr', sep=''))
plot_clulstered_trip_duration_distr(trip_duration_dist, paste(plots_location, 'trip_duration_dist', sep=''))
plot_clulstered_fare_amount_distr(fare_amount_distr, paste(plots_location, 'fare_amount_distr', sep=''))
plot_clulstered_ratecode_distr(ratecode_distr, paste(plots_location, 'ratecode_distr', sep=''))
plot_clulstered_payment_type_distr(payment_type_distr, paste(plots_location, 'payment_type_distr', sep=''))
plot_clulstered_dispute_distr(payment_type_distr, paste(plots_location, 'dispute_distr', sep=''))
plot_clulstered_tolls_amount_distr(tolls_amount_distr, paste(plots_location, 'tolls_amount_distr', sep=''))
plot_clulstered_passenger_count_distr(passenger_count_distr, paste(plots_location, 'passenger_count_distr', sep=''))
plot_clulstered_avg_tip_percentage(avg_tip_percentage, paste(plots_location, 'avg_tip_percentage', sep=''))
plot_clulstered_pickup_hour_dist(pickup_hour_dist, paste(plots_location, 'pickup_hour_dist', sep=''))
plot_clulstered_pickup_weekday_dist(pickup_weekday_dist, paste(plots_location, 'pickup_weekday_dist', sep=''))
plot_clulstered_pickup_location_id_dist(pickup_location_id_dist, shapefile_df, paste(plots_location, 'pickup_location_id_dist', sep=''))
plot_clulstered_dropoff_location_id_dist(dropoff_location_id_dist, shapefile_df, paste(plots_location, 'dropoff_location_id_dist', sep=''))

plot_elbow(elbow, paste(plots_location, 'elbow', sep=''))
