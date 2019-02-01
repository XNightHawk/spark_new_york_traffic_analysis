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
#  Plotting library for main data
#

library(ggplot2)
library(RColorBrewer)
library(rgdal)
library(ggmap)


label_conversion = function(breaks) {
  #breaks = breaks[2:length(breaks)]
  print(breaks)
  labels = NULL
  
  for(current_break in breaks) {
    digits = floor(log10(current_break))
    print(floor(log10(current_break)))
    current_break = current_break / (10 ** digits)
    current_break = as.numeric(format(round(current_break, 2), nsmall = 2))
    current_break = current_break * (10 ** digits)
    labels = c(labels, current_break)
    
    print(labels)
  }
  
  return(labels)
}

plot_airport_distr = function(data, save_name='') {
  plot = ggplot(data, aes(x=class, y=count)) + 
    geom_histogram(stat="identity") +
    xlab(NULL) +
    ylab("Occurrences") +
    scale_x_discrete(labels=c("JFK", "LaGuardia", "Newark", "Standard Trip"))
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
}

plot_avg_total_amount_by_pickup_location = function(data, shapefile_df, save_name='') {
  
  plot = ggplot(data, aes(x=pickup_location_id, y=avg.total_amount.)) +
    geom_point() +
    geom_line()
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
  avg_total_amount_map_df = data
  colnames(avg_total_amount_map_df) <- c("x", "id", "avg_")
  map_df = merge(shapefile_df, avg_total_amount_map_df, by = "id")
  map_df = map_df[order(map_df$order, decreasing = FALSE), ]
  # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
  # Paths handle clipping better. Polygons can be filled.
  # You need the aesthetics long, lat, and group.
  map <- ggplot() +
    geom_polygon(data = map_df, 
                 aes(x = long, y = lat, group = group, fill = avg_),
                 color = 'gray', size = .2) +
    scale_fill_gradientn(colours=c("black", "blue"), limits = c(0, 70))
  
  plot = map
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '_map.pdf', sep='')) 
  }
  print(plot)
  
}

plot_avg_distance_by_pickup_location = function(data, shapefile_df, save_name='') {
  
  plot = ggplot(data, aes(x=pickup_location_id, y=avg.trip_distance.)) +
    geom_histogram(stat="identity")
  
  print(nchar(save_name))
  print(paste(save_name, '.pdf', sep=''))
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep=''), plot) 
  }
  print(plot)
  
  avg_distance_map_df = data
  colnames(avg_distance_map_df) <- c("x", "id", "avg_distance")
  map_df = merge(shapefile_df, avg_distance_map_df, by = "id")
  map_df = map_df[order(map_df$order, decreasing = FALSE), ]
  # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
  # Paths handle clipping better. Polygons can be filled.
  # You need the aesthetics long, lat, and group.
  map <- ggplot() +
    geom_polygon(data = map_df, 
                 aes(x = long, y = lat, group = group, fill = avg_distance),
                 color = 'gray', size = .2) +
    scale_fill_gradientn(colours=c("black", "blue"), trans = "log", name = "distance [miles]", breaks=c(1, 2, 5, 10, 15)) + theme(
      axis.text.x = element_blank(),
      axis.text.y = element_blank(),
      axis.ticks = element_blank()) +
    xlab(NULL) +
    ylab(NULL)
  plot = map
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '_map.pdf', sep='')) 
  }
  print(plot)
  
}
plot_avg_duration_by_pickup_location = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=pickup_location_id, y=avg.duration_seconds.)) +
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
  avg_total_amount_map_df = data
  colnames(avg_total_amount_map_df) <- c("x", "id", "avg_")
  map_df = merge(shapefile_df, avg_total_amount_map_df, by = "id")
  map_df = map_df[order(map_df$order, decreasing = FALSE), ]
  # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
  # Paths handle clipping better. Polygons can be filled.
  # You need the aesthetics long, lat, and group.
  map <- ggplot() +
    geom_polygon(data = map_df, 
                 aes(x = long, y = lat, group = group, fill = avg_),
                 color = 'gray', size = .2) +
    scale_fill_gradientn(colours=c("black", "blue"))
  
  plot = map
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '_map.pdf', sep='')) 
  }
  print(plot)
  
}
plot_avg_extra_by_hour = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=hour, y=avg.extra.)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_avg_fare_amount_percentage = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=year, y=avg.fare_amount_percentage.)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_avg_speed_by_pickup_hour_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=pickup_hour, y=avg.average_speed.)) + 
    geom_histogram(stat="identity") +
    xlab("Pickup hour") +
    ylab("Average speed [mph]") +
    scale_x_discrete(limits = c(0, 6, 12, 18, 23))
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_avg_speed_by_pickup_location = function(data, shapefile_df, save_name='') {
  
  plot = ggplot(data, aes(x=pickup_location_id, y=avg.avg_speed.)) + 
    geom_histogram(stat="identity")
  
    
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  
  print(plot)
  
  avg_speed_map_df = data
  colnames(avg_speed_map_df) <- c("x", "id", "avg_speed")
  map_df = merge(shapefile_df, avg_speed_map_df, by = "id")
  map_df = map_df[order(map_df$order, decreasing = FALSE), ]
  # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
  # Paths handle clipping better. Polygons can be filled.
  # You need the aesthetics long, lat, and group.
  map <- ggplot() +
    geom_polygon(data = map_df, 
                 aes(x = long, y = lat, group = group, fill = avg_speed),
                 color = 'gray', size = .2) +
    scale_fill_gradientn(colours=c("black", "blue"), trans = "log", name = "Avg speed [mph]", breaks=c(7, 10, 20)) + theme(
      axis.text.x = element_blank(),
      axis.text.y = element_blank(),
      axis.ticks = element_blank()) +
    xlab(NULL) +
    ylab(NULL)
  
  plot = map
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '_map.pdf', sep='')) 
  }
  
  print(plot)
  
}
plot_avg_tip_percentage = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=X, y=avg.tip_percentage.)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_avg_tip_percentage_by_year = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=year, y=avg.tip_percentage.)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_avg_total_amount_by_month = function(data, save_name='') {
  
  plot = ggplot(data, aes(factor(x=month.pickup_datetime.), y=avg.total_amount.)) + 
    geom_histogram(stat="identity") +
    xlab("Month") +
    ylab("Average total amount [$]") +
    scale_x_discrete(breaks = c(1, 3, 6, 9, 12), labels=c("January", "March", "June", "Semptember", "December"))
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_avg_total_amount_by_pickup_hour = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=hour.pickup_datetime., y=avg.total_amount.)) + 
    geom_histogram(stat="identity") +
    xlab("Pickup hour") +
    ylab("Average total amount [$]") +
    scale_x_discrete(limits = c(0, 6, 12, 18, 23))
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_avg_total_amount_by_pickup_location = function(data, shapefile_df, save_name='') {
  
  avg_total_amount_map_df = data
  colnames(avg_total_amount_map_df) <- c("x", "id", "avg_")
  map_df = merge(shapefile_df, avg_total_amount_map_df, by = "id")
  map_df = map_df[order(map_df$order, decreasing = FALSE), ]
  # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
  # Paths handle clipping better. Polygons can be filled.
  # You need the aesthetics long, lat, and group.
  map <- ggplot() +
    geom_polygon(data = map_df, 
                 aes(x = long, y = lat, group = group, fill = avg_),
                 color = 'gray', size = .2) +
    scale_fill_gradientn(colours=c("black", "blue"), trans = "log", name = "avg total amount [$]", breaks=c(70, 40, 20, 10)) + theme(
      axis.text.x = element_blank(),
      axis.text.y = element_blank(),
      axis.ticks = element_blank()) +
    xlab(NULL) +
    ylab(NULL)
  
  plot = map
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '_map.pdf', sep='')) 
    }
  print(plot)
  
}
plot_avg_total_amount_by_year = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=year.pickup_datetime., y=avg.total_amount.)) + 
    geom_histogram(stat="identity") +
    xlab("Year") +
    ylab("Average total amount [$]")
    
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_dropoff_location_by_pickup_hour_distr = function(data, save_name='') {
  
  for(current_hour in c(0, 3, 5, 7, 9, 10, 12, 14, 16, 18, 20, 22, 23)) {
    # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
    # Paths handle clipping better. Polygons can be filled.
    # You need the aesthetics long, lat, and group.
    current_data = data[data$hour.pickup_datetime. == current_hour, ]
    
    
    pickup_location_df = current_data
    
    colnames(pickup_location_df) <- c("x", "hour", "id", "count")
    
    map_df = merge(shapefile_df, pickup_location_df, by = "id")
    map_df = map_df[order(map_df$order, decreasing = FALSE), ]
    
    map <- ggplot() +
      geom_polygon(data = map_df, 
                   aes(x = long, y = lat, group = group, fill = count),
                   color = 'gray', size = .2) +
      #scale_fill_gradientn(colours = rainbow(5), trans = "log")
      #scale_fill_grey(start=0.8, end=0.2)
      scale_fill_gradientn(colours=c("black", "blue"), name = "dropoffs #", labels=label_conversion) + theme(
        axis.text.x = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks = element_blank()) +
      xlab(NULL) +
      ylab(NULL)
    plot = map
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, current_hour, '_map.pdf', sep='')) 
    }
    print(plot)
  }
  
  plot = ggplot(data, aes(x=dropoff_location_id, y=count)) +
    geom_histogram(stat="identity") +
    facet_wrap( ~ hour.pickup_datetime., ncol=2)
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}


plot_dropoff_location_id_dist = function(data, shapefile_df, save_name='') {
  
  dropoff_location_df = data
  colnames(dropoff_location_df) <- c("x", "id", "count")
  map_df = merge(shapefile_df, dropoff_location_df, by = "id")
  map_df = map_df[order(map_df$order, decreasing = FALSE), ]
  # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
  # Paths handle clipping better. Polygons can be filled.
  # You need the aesthetics long, lat, and group.
  map <- ggplot() +
    geom_polygon(data = map_df, 
                 aes(x = long, y = lat, group = group, fill = count),
                 color = 'gray', size = .2) +
    #scale_fill_gradientn(colours = rainbow(5), trans = "log")
    #scale_fill_grey(start=0.8, end=0.2)
    scale_fill_gradientn(colours=c("black", "blue"), trans = "log", name = "dropoffs #", labels=label_conversion) + theme(
      axis.text.x = element_blank(),
      axis.text.y = element_blank(),
      axis.ticks = element_blank()) +
    xlab(NULL) +
    ylab(NULL)
  plot = map
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '_map.pdf', sep='')) 
    }
  print(plot)
  
}
plot_extra_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=extra, y=count)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_fare_amount_by_company_distr = function(data, save_name='') {
  
  plot =  ggplot(data, aes(x=fare_amount, y=count, fill=taxi_company)) + 
    geom_histogram(stat="identity", position="dodge") +
    xlim(0, 65) +
    xlab("Fare amount [$]") +
    ylab("Occurrences") +
    scale_fill_discrete(name = "taxi type")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_fare_amount_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=CAST.fare_amount.AS.INT., y=count)) + 
    geom_histogram(stat="identity") +
    xlim(c(0, 60)) +
    xlab("Fare amount [$]") +
    ylab("Occurrences")
    
    
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_fhv_vs_taxi = function(data, save_name='') {
  
  data = data[data$year.date. >= 2010,]
  data = data[is.na(data$parent) == FALSE,]
    
  data$index = (data$year.date. - 2010) * 365 + data$dayofyear.date.
  data = data[data$index <= ((2018 - 2010) * 365 + (365 / 2)),]
  
  label = 2010:2018
  years = ((label) - 2010) * 365
  
  plot = ggplot(data, aes(x=index, y=sum.count., color=factor(parent))) + 
    geom_line() +
    scale_color_discrete(name = "Service type") +
    scale_x_continuous(breaks=years, labels=label) +
    xlab(NULL) +
    ylab("Pickups #")

  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_improvement_surcharge_by_year = function(data, save_name='') {
  
  plot =ggplot(data, aes(x=year.pickup_datetime., y=sum.improvement_surcharge.)) + 
    geom_histogram(stat="identity")
    
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_improvement_surcharge_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=improvement_surcharge, y=count)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_monthly_profit_by_year = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=month, y=X.sum.total_amount....sum.total_amount..)) + 
    geom_histogram(stat="identity") +
    facet_wrap( ~ year, ncol=2)
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_mta_tax_by_year = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=year.pickup_datetime., y=sum.mta_tax.)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_mta_tax_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=mta_tax, y=count)) + 
    geom_histogram(stat="identity")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_overall_fare_amount = function(data, save_name='') {
  
  data$index = (data$year - 2010) * 365 + data$day
  data = data[data$index <= ((2018 - 2010) * 365 + (365 / 2)),]
  
  label = 2010:2018
  years = ((label) - 2010) * 365
  
  plot = ggplot(data, aes(x=index, y=avg.fare_amount.)) + 
    geom_line() +
    scale_x_continuous(breaks=years, labels=label) +
    xlab(NULL) +
    ylab("Average fare amount [$]") +
    ylim(0, 18)
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_overall_dropoffs = function(data, save_name='') {
  
  data$index = (data$year - 2010) * 365 + data$day
  data = data[data$index <= ((2018 - 2010) * 365 + (365 / 2)),]
  
  label = 2010:2018
  years = ((label) - 2010) * 365
  
  plot = ggplot(data, aes(x=index, y=count)) + 
    geom_line() +
    scale_x_continuous(breaks=years, labels=label) +
    xlab(NULL) +
    ylab("Dropoffs #")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_overall_pickups = function(data, save_name='') {
  
  data$index = (data$year - 2010) * 365 + data$day
  data = data[data$index <= ((2018 - 2010) * 365 + (365 / 2)),]
  
  label = 2010:2018
  years = ((label) - 2010) * 365
  
  plot = ggplot(data, aes(x=index, y=count)) + 
    geom_line() +
    scale_x_continuous(breaks=years, labels=label) +
    xlab(NULL) +
    ylab("Pickups #")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_overall_total_amount = function(data, save_name='') {
  
  data$index = (data$year - 2010) * 365 + data$day
  data = data[data$index <= ((2018 - 2010) * 365 + (365 / 2)),]
  
  label = 2010:2018
  years = ((label) - 2010) * 365
  
  plot = ggplot(data, aes(x=index, y=avg.total_amount.)) + 
    geom_line() +
    scale_x_continuous(breaks=years, labels=label) +
    xlab(NULL) +
    ylab("Average total amount [$]") +
    ylim(0, 20)
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_passenger_count_by_year_and_company_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=passenger_count, y=count, fill=taxi_company)) + 
    geom_histogram(stat="identity", position="dodge") +
    facet_wrap( ~ year, ncol=5)
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_passenger_count_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=passenger_count, y=count)) + 
    geom_histogram(stat="identity") +
    xlab("Passenger #") +
    ylab("Occurrences")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_payment_type_distr = function(data, save_name='') {
  
  data = data[data$payment_type == 1 | data$payment_type == 2,]
  
  plot = ggplot(data, aes(x=factor(payment_type), y=count)) + 
    geom_histogram(stat="identity") +
    xlab(NULL) +
    ylab("Occurrences") +
    facet_wrap( ~ year, ncol=9) +
    scale_x_discrete(breaks = c(1, 2), labels=c("CC", "Cash"))
  
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_pickup_dropoff_location_by_pickup_hour_distr = function(data, save_name='') {
  
  plot = ggplot(data,
                aes(x = pickup_location_id, y = dropoff_location_id, z = count)) +
    geom_raster(aes(fill = count), hjust=0.5, vjust=0.5, interpolate=FALSE) +
    scale_fill_gradientn(colours=c("black","red"), trans = "log") +
    facet_wrap( ~ hour.pickup_datetime., ncol=8)
    
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_pickup_hour_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=pickup_hour, y=count)) + 
    geom_histogram(stat="identity") +
    xlab("Pickup hour") +
    ylab("Occurrences") +
    scale_x_discrete(limits = c(0, 6, 12, 18, 23))
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_pickup_location_by_company_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=pickup_location_id, y=count, fill=taxi_company)) + 
    geom_histogram(stat="identity", position="dodge") +
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
  for(current_company in c("yellow", "green")) {
    current_data = data[data$taxi_company == current_company, ]
    
    dropoff_location_df = current_data
    colnames(dropoff_location_df) <- c("x", "id", "company", "count")
    map_df = merge(shapefile_df, dropoff_location_df, by = "id")
    map_df = map_df[order(map_df$order, decreasing = FALSE), ]
    # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
    # Paths handle clipping better. Polygons can be filled.
    # You need the aesthetics long, lat, and group.
    map <- ggplot() +
      geom_polygon(data = map_df, 
                   aes(x = long, y = lat, group = group, fill = count),
                   color = 'gray', size = .2) +
      #scale_fill_gradientn(colours = rainbow(5), trans = "log")
      #scale_fill_grey(start=0.8, end=0.2)
      scale_fill_gradientn(colours=c("black", "blue"), trans="log")
    #scale_fill_gradient2(low='gold',mid = "white",high='firebrick2',na.value = "lightgrey",midpoint=25,limits=c(5, 30), name='Avg speed')
    
    plot = map
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, "_company_", current_company, '_map.pdf', sep='')) 
    }
    print(plot)
  }
  
  yellow_data = data[data$taxi_company == "yellow", ]
  green_data = data[data$taxi_company == "green", ]
  
  merged_df = merge(yellow_data, green_data, by = "pickup_location_id")
  
  merged_df$difference = as.numeric((merged_df$count.x - merged_df$count.y) >= 0)
  
  dropoff_location_df = merged_df
  #colnames(dropoff_location_df) <- c("x", "id", "company", "count")
  dropoff_location_df$id = dropoff_location_df$pickup_location_id
  map_df = merge(shapefile_df, dropoff_location_df, by = "id")
  map_df = map_df[order(map_df$order, decreasing = FALSE), ]
  # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
  # Paths handle clipping better. Polygons can be filled.
  # You need the aesthetics long, lat, and group.
  map <- ggplot() +
    geom_polygon(data = map_df, 
                 aes(x = long, y = lat, group = group, fill = difference),
                 color = 'gray', size = .2) +
    #scale_fill_gradientn(colours = rainbow(5), trans = "log")
    #scale_fill_grey(start=0.8, end=0.2)
    scale_fill_gradientn(colours=c("green", "yellow"), name = "taxi type") + theme(
    legend.position="none",
    axis.text.x = element_blank(),
    axis.text.y = element_blank(),
    axis.ticks = element_blank()) +
    xlab(NULL) +
    ylab(NULL)
  
  plot = map
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '_difference_map.pdf', sep='')) 
  }
  print(plot)
}
plot_pickup_location_by_pickup_hour_distr = function(data, save_name='') {
  
  for(current_hour in c(0, 3, 5, 7, 9, 10, 12, 14, 16, 18, 20, 22, 23)) {
    # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
    # Paths handle clipping better. Polygons can be filled.
    # You need the aesthetics long, lat, and group.
    current_data = data[data$hour.pickup_datetime. == current_hour, ]
    
    
    pickup_location_df = current_data
    
    colnames(pickup_location_df) <- c("x", "hour", "id", "count")
    
    map_df = merge(shapefile_df, pickup_location_df, by = "id")
    map_df = map_df[order(map_df$order, decreasing = FALSE), ]
    
    map <- ggplot() +
      geom_polygon(data = map_df, 
                   aes(x = long, y = lat, group = group, fill = count),
                   color = 'gray', size = .2) +
      #scale_fill_gradientn(colours = rainbow(5), trans = "log")
      #scale_fill_grey(start=0.8, end=0.2)
      scale_fill_gradientn(colours=c("black", "blue"))
    #scale_fill_gradient2(low='gold',mid = "white",high='firebrick2',na.value = "lightgrey",midpoint=25,limits=c(5, 30), name='Avg speed')
    
    plot = map
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, current_hour, '_', current_hour , '_map.pdf', sep='')) 
    }
    print(plot)
  }
  
  plot = ggplot(data, aes(x=pickup_location_id, y=count)) +
  geom_histogram(stat="identity") +
    facet_wrap( ~ hour.pickup_datetime., ncol=2)
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}

plot_pickup_location_id_dist = function(data, shapefile_df, save_name='') {
  
  pickup_location_df = data
  colnames(pickup_location_df) <- c("x", "id", "occurrences")
  map_df = merge(shapefile_df, pickup_location_df, by = "id")
  map_df = map_df[order(map_df$order, decreasing = FALSE), ]
  # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
  # Paths handle clipping better. Polygons can be filled.
  # You need the aesthetics long, lat, and group.
  map <- ggplot() +
    geom_polygon(data = map_df, 
                 aes(x = long, y = lat, group = group, fill = occurrences),
                 color = 'gray', size = .2) +
    #scale_fill_gradientn(colours = rainbow(5), trans = "log")
    #scale_fill_grey(start=0.8, end=0.2)
    scale_fill_gradientn(colours=c("black", "blue"), trans = "log", name = "pickups #", labels=label_conversion) + theme(
      axis.text.x = element_blank(),
      axis.text.y = element_blank(),
      axis.ticks = element_blank()) +
    xlab(NULL) +
    ylab(NULL)
  
  plot = map
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '_map.pdf', sep='')) 
    }
  print(plot)
  
}
plot_pickup_month_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=pickup_month, y=count)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_pickup_yearday_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=pickup_day, y=count)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_profits_by_year = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=year.pickup_datetime., y=sum.total_amount.)) + 
    geom_histogram(stat="identity") +
    xlab("Year") +
    ylab("Profits [$]")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_profits_by_year_and_company = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=year, y=sum.total_amount., fill=taxi_company)) + 
    geom_histogram(stat="identity", position="dodge") +
    scale_fill_discrete(name = "taxi company") +
    xlab("Year") +
    ylab("Profits [$]")
  
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_ratecode_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=ratecode_id, y=count)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_tips_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=tip_amount, y=count)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_tolls_amount_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=CAST.tolls_amount.AS.INT., y=count)) + 
    geom_histogram(stat="identity") +
    xlab("Tolls amount [$]") +
    ylab("Occurrences") +
    xlim(-1, 8)
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}

plot_total_amount_by_airport = function(data, save_name='') {
  plot = ggplot(data, aes(x=class, y=sum.total_amount.)) + 
    geom_histogram(stat="identity") +
    xlab(NULL) +
    ylab("Total profits [$]") +
    scale_x_discrete(labels=c("JFK", "LaGuardia", "Newark", "Standard Trip"))
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
}


plot_total_amount_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=CAST.total_amount.AS.INT., y=count)) +
  geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_travels_by_year = function(data, save_name='') {
  
  plot = plot = ggplot(data, aes(x=year, y=count)) + 
    geom_histogram(stat="identity") +
    xlab("Year") +
    ylab("Occurrences")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_travels_by_year_and_company = function(data, save_name='') {
  
  plot = plot = ggplot(data, aes(x=year, y=count, fill=taxi_company)) + 
    geom_histogram(stat="identity", position="stack") +
    xlab("Year") +
    ylab("Occurrences") +
    scale_fill_discrete(name="taxi company")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_trip_distance_by_company_distr = function(data, save_name='') {
  
  plot =  ggplot(data, aes(x=distance, y=count, fill=taxi_company)) + 
    geom_histogram(stat="identity", position="dodge") +
    xlim(0, 25) +
    xlab("Distane [miles]") +
    ylab("Occurrences") +
    scale_fill_discrete(name = "taxi company")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_trip_distance_by_pickup_hour_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=CAST.trip_distance.AS.INT., y=count)) + 
    geom_histogram(stat="identity") +
    facet_wrap( ~ hour.pickup_datetime., ncol=8) +
    xlim(0, 30)
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_trip_distance_by_year_and_company_distr = function(data, save_name='') {
  
  plot =  ggplot(data, aes(x=distance, y=count, fill=taxi_company)) + 
    geom_histogram(stat="identity", position="dodge") +
    xlim(0, 30) +
    facet_wrap( ~ year, ncol=5)
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_trip_distance_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=CAST.trip_distance.AS.INT., y=count)) +
    geom_histogram(stat="identity") +
    xlab("Trip distance [miles]") +
    ylab("Occurrences") +
    xlim(0, 25)
    
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
plot_trip_duration_by_pickup_hour_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=trip_duration_minutes, y=count)) + 
    geom_histogram(stat="identity") +
    facet_wrap( ~ hour.pickup_datetime., ncol=8)
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}
plot_trip_duration_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=trip_duration_minutes, y=count)) + 
    geom_histogram(stat="identity")
    
    if(nchar(save_name) > 1) {
      ggsave(paste(save_name, '.pdf', sep='')) 
    }
  print(plot)
  
}