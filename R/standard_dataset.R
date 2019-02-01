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
#  Reads results of main data analysis
#

avg_distance_by_pickup_location = read.csv(paste(dataset_location, "avg_distance_by_pickup_location.csv", sep='')) 
avg_duration_by_pickup_location = read.csv(paste(dataset_location, "avg_duration_by_pickup_location.csv", sep='')) 
avg_extra_by_hour = read.csv(paste(dataset_location, "avg_extra_by_hour.csv", sep=''))
avg_fare_amount_percentage = read.csv(paste(dataset_location, "avg_fare_amount_percentage.csv", sep=''))
avg_speed_by_pickup_hour_dist = read.csv(paste(dataset_location, "avg_speed_by_pickup_hour_dist.csv", sep='')) 
avg_speed_by_pickup_location = read.csv(paste(dataset_location, "avg_speed_by_pickup_location.csv", sep=''))
avg_tip_percentage = read.csv(paste(dataset_location, "avg_tip_percentage.csv", sep='')) # No need to plot
avg_tip_percentage_by_year = read.csv(paste(dataset_location, "avg_tip_percentage_by_year.csv", sep='')) 
avg_total_amount_by_month = read.csv(paste(dataset_location, "avg_total_amount_by_month.csv", sep=''))
avg_total_amount_by_pickup_hour = read.csv(paste(dataset_location, "avg_total_amount_by_pickup_hour.csv", sep=''))
avg_total_amount_by_pickup_location = read.csv(paste(dataset_location, "avg_total_amount_by_pickup_location.csv", sep=''))
avg_total_amount_by_year = read.csv(paste(dataset_location, "avg_total_amount_by_year.csv", sep=''))
bronx_dropoff_location_id_by_pickup_hour_dist = read.csv(paste(dataset_location, "bronx_dropoff_location_id_by_pickup_hour_dist.csv", sep=''))
bronx_dropoff_location_id_dist = read.csv(paste(dataset_location, "bronx_dropoff_location_id_dist.csv", sep=''))
brooklyn_dropoff_location_id_by_pickup_hour_dist = read.csv(paste(dataset_location, "brooklyn_dropoff_location_id_by_pickup_hour_dist.csv", sep=''))
brooklyn_dropoff_location_id_dist = read.csv(paste(dataset_location, "brooklyn_dropoff_location_id_dist.csv", sep=''))
dropoff_location_by_pickup_hour_distr = read.csv(paste(dataset_location, "dropoff_location_by_pickup_hour_distr.csv", sep=''))
dropoff_location_distr = read.csv(paste(dataset_location, "dropoff_location_distr.csv", sep=''))
dropoff_location_id_dist = read.csv(paste(dataset_location, "dropoff_location_id_dist.csv", sep=''))
extra_distr = read.csv(paste(dataset_location, "extra_distr.csv", sep=''))
fare_amount_by_company_distr = read.csv(paste(dataset_location, "fare_amount_by_company_distr.csv", sep=''))
fare_amount_distr = read.csv(paste(dataset_location, "fare_amount_distr.csv", sep=''))
fhv_vs_taxi = read.csv(paste(dataset_location, "fhv_vs_taxi.csv", sep=''))
improvement_surcharge_by_year = read.csv(paste(dataset_location, "improvement_surcharge_by_year.csv", sep=''))
improvement_surcharge_distr = read.csv(paste(dataset_location, "improvement_surcharge_distr.csv", sep=''))
manhattan_dropoff_location_id_by_pickup_hour_dist = read.csv(paste(dataset_location, "manhattan_dropoff_location_id_by_pickup_hour_dist.csv", sep=''))
manhattan_dropoff_location_id_dist = read.csv(paste(dataset_location, "manhattan_dropoff_location_id_dist.csv", sep=''))
monthly_profit_by_year = read.csv(paste(dataset_location, "monthly_profit_by_year.csv", sep=''))
mta_tax_by_year = read.csv(paste(dataset_location, "mta_tax_by_year.csv", sep=''))
mta_tax_distr = read.csv(paste(dataset_location, "mta_tax_distr.csv", sep=''))
overall_fare_amount = read.csv(paste(dataset_location, "overall_fare_amount.csv", sep=''))
overall_dropoffs = read.csv(paste(dataset_location, "overall_dropoffs.csv", sep=''))
overall_pickups = read.csv(paste(dataset_location, "overall_pickups.csv", sep=''))
overall_total_amount = read.csv(paste(dataset_location, "overall_total_amount.csv", sep=''))
passenger_count_by_year_and_company_distr = read.csv(paste(dataset_location, "passenger_count_by_year_and_company_distr.csv", sep=''))
passenger_count_dist = read.csv(paste(dataset_location, "passenger_count_dist.csv", sep=''))
passenger_count_distr = read.csv(paste(dataset_location, "passenger_count_distr.csv", sep=''))
payment_type_distr = read.csv(paste(dataset_location, "payment_type_distr.csv", sep=''))
pickup_dropoff_location_by_pickup_hour_distr = read.csv(paste(dataset_location, "pickup_dropoff_location_by_pickup_hour_distr.csv", sep=''))
pickup_hour_dist = read.csv(paste(dataset_location, "pickup_hour_dist.csv", sep=''))
pickup_location_by_company_after_2014_distr = read.csv(paste(dataset_location, "pickup_location_by_company_after_2014_distr.csv", sep=''))
pickup_location_by_company_distr = read.csv(paste(dataset_location, "pickup_location_by_company_distr.csv", sep=''))
pickup_location_by_pickup_hour_distr = read.csv(paste(dataset_location, "pickup_location_by_pickup_hour_distr.csv", sep=''))
pickup_location_distr = read.csv(paste(dataset_location, "pickup_location_distr.csv", sep=''))
pickup_location_id_dist = read.csv(paste(dataset_location, "pickup_location_id_dist.csv", sep=''))
pickup_month_dist = read.csv(paste(dataset_location, "pickup_month_dist.csv", sep=''))
pickup_yearday_dist = read.csv(paste(dataset_location, "pickup_yearday_dist.csv", sep=''))
profits_by_year = read.csv(paste(dataset_location, "profits_by_year.csv", sep=''))
profits_by_year_and_company = read.csv(paste(dataset_location, "profits_by_year_and company.csv", sep=''))
queens_dropoff_location_id_by_pickup_hour_dist = read.csv(paste(dataset_location, "queens_dropoff_location_id_by_pickup_hour_dist.csv", sep=''))
queens_dropoff_location_id_dist = read.csv(paste(dataset_location, "queens_dropoff_location_id_dist.csv", sep=''))
ratecode_distr = read.csv(paste(dataset_location, "ratecode_distr.csv", sep=''))
staten_island_dropoff_location_id_by_pickup_hour_dist = read.csv(paste(dataset_location, "staten_island_dropoff_location_id_by_pickup_hour_dist.csv", sep=''))
staten_island_dropoff_location_id_dist = read.csv(paste(dataset_location, "staten_island_dropoff_location_id_dist.csv", sep=''))
tips_distr = read.csv(paste(dataset_location, "tips_distr.csv", sep=''))
tolls_amount_distr = read.csv(paste(dataset_location, "tolls_amount_distr.csv", sep=''))
total_amount_distr = read.csv(paste(dataset_location, "total_amount_distr.csv", sep=''))
travels_by_year_and_company = read.csv(paste(dataset_location, "travels_by_year_and company.csv", sep=''))
trip_distance_by_company_distr = read.csv(paste(dataset_location, "trip_distance_by_company_distr.csv", sep=''))
trip_distance_by_pickup_hour_dist = read.csv(paste(dataset_location, "trip_distance_by_pickup_hour_dist.csv", sep=''))
trip_distance_by_year_and_company_distr = read.csv(paste(dataset_location, "trip_distance_by_year_and company_distr.csv", sep=''))
trip_distance_distr = read.csv(paste(dataset_location, "trip_distance_distr.csv", sep=''))
trip_duration_by_pickup_hour_dist = read.csv(paste(dataset_location, "trip_duration_by_pickup_hour_dist.csv", sep=''))
trip_duration_dist = read.csv(paste(dataset_location, "trip_duration_dist.csv", sep=''))

travels_by_year = merge(travels_by_year_and_company, travels_by_year_and_company, by="year")
travels_by_year = travels_by_year[travels_by_year$taxi_company.x == "yellow",]
travels_by_year$count = travels_by_year$count.x + travels_by_year$count.y