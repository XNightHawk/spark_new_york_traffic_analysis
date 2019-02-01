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
#  Reads results of clustered data specific analysis
#

avg_distance_by_pickup_location = read.csv(paste(dataset_location, "avg_distance_by_pickup_location.csv", sep=''))
avg_duration_by_pickup_location = read.csv(paste(dataset_location, "avg_duration_by_pickup_location.csv", sep=''))
avg_extra_by_hour = read.csv(paste(dataset_location, "avg_extra_by_hour.csv", sep=''))
avg_fare_amount_percentage = read.csv(paste(dataset_location, "avg_fare_amount_percentage.csv", sep=''))
avg_speed_by_pickup_hour_dist = read.csv(paste(dataset_location, "avg_speed_by_pickup_hour_dist.csv", sep=''))
avg_speed_by_pickup_location = read.csv(paste(dataset_location, "avg_speed_by_pickup_location.csv", sep=''))
avg_tip_percentage = read.csv(paste(dataset_location, "avg_tip_percentage.csv", sep=''))
avg_tip_percentage_by_year = read.csv(paste(dataset_location, "avg_tip_percentage_by_year.csv", sep=''))
avg_total_amount_by_month = read.csv(paste(dataset_location, "avg_total_amount_by_month.csv", sep=''))
avg_total_amount_by_pickup_hour = read.csv(paste(dataset_location, "avg_total_amount_by_pickup_hour.csv", sep=''))
avg_total_amount_by_pickup_location = read.csv(paste(dataset_location, "avg_total_amount_by_pickup_location.csv", sep=''))
avg_total_amount_by_year = read.csv(paste(dataset_location, "avg_total_amount_by_year.csv", sep=''))
dayofweek_mean = read.csv(paste(dataset_location, "dayofweek_mean.csv", sep=''))
dayofweek_variance = read.csv(paste(dataset_location, "dayofweek_variance.csv", sep=''))
distance_mean = read.csv(paste(dataset_location, "distance_mean.csv", sep=''))
distance_variance = read.csv(paste(dataset_location, "distance_variance.csv", sep=''))
dropoff_hour_mean = read.csv(paste(dataset_location, "dropoff_hour_mean.csv", sep=''))
dropoff_hour_variance = read.csv(paste(dataset_location, "dropoff_hour_variance.csv", sep=''))
dropoff_location_by_pickup_hour_distr = read.csv(paste(dataset_location, "dropoff_location_by_pickup_hour_distr.csv", sep=''))
dropoff_location_distr = read.csv(paste(dataset_location, "dropoff_location_distr.csv", sep=''))
dropoff_location_id_dist = read.csv(paste(dataset_location, "dropoff_location_id_dist.csv", sep=''))
extra_distr = read.csv(paste(dataset_location, "extra_distr.csv", sep=''))
fare_amount_distr = read.csv(paste(dataset_location, "fare_amount_distr.csv", sep=''))
fare_amount_mean = read.csv(paste(dataset_location, "fare_amount_mean.csv", sep=''))
fare_amount_variance = read.csv(paste(dataset_location, "fare_amount_variance.csv", sep=''))
improvement_surcharge_by_year = read.csv(paste(dataset_location, "improvement_surcharge_by_year.csv", sep=''))
improvement_surcharge_distr = read.csv(paste(dataset_location, "improvement_surcharge_distr.csv", sep=''))
monthly_profit_by_year = read.csv(paste(dataset_location, "monthly_profit_by_year.csv", sep=''))
mta_tax_by_year = read.csv(paste(dataset_location, "mta_tax_by_year.csv", sep=''))
mta_tax_distr = read.csv(paste(dataset_location, "mta_tax_distr.csv", sep=''))
passenger_count_by_year_and_company_distr = read.csv(paste(dataset_location, "passenger_count_by_year_and_company_distr.csv", sep=''))
passenger_count_dist = read.csv(paste(dataset_location, "passenger_count_dist.csv", sep=''))
passenger_count_distr = read.csv(paste(dataset_location, "passenger_count_distr.csv", sep=''))
passenger_count_mean = read.csv(paste(dataset_location, "passenger_count_mean.csv", sep=''))
passenger_count_variance = read.csv(paste(dataset_location, "passenger_count_variance.csv", sep=''))
payment_type_distr = read.csv(paste(dataset_location, "payment_type_distr.csv", sep=''))
payment_type_mean = read.csv(paste(dataset_location, "payment_type_mean.csv", sep=''))
payment_type_variance = read.csv(paste(dataset_location, "payment_type_variance.csv", sep=''))
pickup_dropoff_location_by_pickup_hour_distr = read.csv(paste(dataset_location, "pickup_dropoff_location_by_pickup_hour_distr.csv", sep=''))
pickup_hour_dist = read.csv(paste(dataset_location, "pickup_hour_dist.csv", sep=''))
pickup_hour_mean = read.csv(paste(dataset_location, "pickup_hour_mean.csv", sep=''))
pickup_hour_variance = read.csv(paste(dataset_location, "pickup_hour_variance.csv", sep=''))
pickup_location_by_company_distr = read.csv(paste(dataset_location, "pickup_location_by_company_distr.csv", sep=''))
pickup_location_by_pickup_hour_distr = read.csv(paste(dataset_location, "pickup_location_by_pickup_hour_distr.csv", sep=''))
pickup_location_distr = read.csv(paste(dataset_location, "pickup_location_distr.csv", sep=''))
pickup_location_id_dist = read.csv(paste(dataset_location, "pickup_location_id_dist.csv", sep=''))
pickup_month_dist = read.csv(paste(dataset_location, "pickup_month_dist.csv", sep=''))
pickup_weekday_dist = read.csv(paste(dataset_location, "pickup_weekday_dist.csv", sep=''))
pickup_yearday_dist = read.csv(paste(dataset_location, "pickup_yearday_dist.csv", sep=''))
profits_by_year = read.csv(paste(dataset_location, "profits_by_year.csv", sep=''))
profits_by_year_and_company = read.csv(paste(dataset_location, "profits_by_year_and company.csv", sep=''))
ratecode_distr = read.csv(paste(dataset_location, "ratecode_distr.csv", sep=''))
ratecode_id_mean = read.csv(paste(dataset_location, "ratecode_id_mean.csv", sep=''))
ratecode_id_variance = read.csv(paste(dataset_location, "ratecode_id_variance.csv", sep=''))
speed_mean = read.csv(paste(dataset_location, "speed_mean.csv", sep=''))
speed_variance = read.csv(paste(dataset_location, "speed_variance.csv", sep=''))
tip_amount_mean = read.csv(paste(dataset_location, "tip_amount_mean.csv", sep=''))
tip_amount_variance = read.csv(paste(dataset_location, "tip_amount_variance.csv", sep=''))
tips_distr = read.csv(paste(dataset_location, "tips_distr.csv", sep=''))
tolls_amount_distr = read.csv(paste(dataset_location, "tolls_amount_distr.csv", sep=''))
tolls_amount_mean = read.csv(paste(dataset_location, "tolls_amount_mean.csv", sep=''))
tolls_amount_variance = read.csv(paste(dataset_location, "tolls_amount_variance.csv", sep=''))
total_amount_by_pickup_hour_dist = read.csv(paste(dataset_location, "total_amount_by_pickup_hour_dist.csv", sep=''))
total_amount_distr = read.csv(paste(dataset_location, "total_amount_distr.csv", sep=''))
travels_by_year_and_company = read.csv(paste(dataset_location, "travels_by_year_and company.csv", sep=''))
trip_distance_by_pickup_hour_dist = read.csv(paste(dataset_location, "trip_distance_by_pickup_hour_dist.csv", sep=''))
trip_distance_by_year_and_company_distr = read.csv(paste(dataset_location, "trip_distance_by_year_and company_distr.csv", sep=''))
trip_distance_distr = read.csv(paste(dataset_location, "trip_distance_distr.csv", sep=''))
trip_distancemean = read.csv(paste(dataset_location, "trip_distancemean.csv", sep=''))
trip_distancevariance = read.csv(paste(dataset_location, "trip_distancevariance.csv", sep=''))
trip_duration_by_pickup_hour_dist = read.csv(paste(dataset_location, "trip_duration_by_pickup_hour_dist.csv", sep=''))
trip_duration_dist = read.csv(paste(dataset_location, "trip_duration_dist.csv", sep=''))


k_values = c(6.54e9, 6.39e9, 4.74e9, 3.89e9, 3.45e9, 3.64e9)

elbow = data.frame(k=2:7, error=k_values)
