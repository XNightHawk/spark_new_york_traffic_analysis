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
#  Plotting library for specific clustered data analysis
#

plot_clulstered_trip_distance_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=CAST.trip_distance.AS.INT., y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=1, position="stack") +
    xlim(0,30)
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_trip_distance_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=CAST.trip_distance.AS.INT., y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=1, position="stack") +
    xlab("Distance [miles]") +
    ylab("Occurrences") +
    xlim(0,30) +
    scale_fill_discrete(name = "cluster")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_trip_duration_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=trip_duration_minutes, y=count, color=factor(cluster))) + 
    #geom_histogram(stat="identity", alpha=0.5, position="identity") +
    geom_density(stat="identity") +
    xlim(0, 100) +
    scale_color_discrete(name = "cluster")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_fare_amount_distr = function(data, save_name='') {
  
  data = data[order(data$cluster, decreasing = TRUE),]
  
  plot = ggplot(data, aes(x=CAST.fare_amount.AS.INT., y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=0.5, position="identity") +
    xlim(0, 150) +
    scale_fill_discrete(name = "cluster")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_ratecode_distr = function(data, save_name='') {
  
  data = data[data$ratecode_id == 1 | data$ratecode_id == 2 | data$ratecode_id == 3,]
  
  plot = ggplot(data, aes(x=factor(ratecode_id), y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=1, position="dodge") +
    scale_x_discrete(breaks = c(1, 2, 3), labels=c("Standard", "JFK", "Newark")) +
    xlab("Fare rate") +
    ylab("Occurrences") +
    scale_fill_discrete(name = "cluster")
    
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_payment_type_distr = function(data, save_name='') {
  
  data = data[data$payment_type == 1 | data$payment_type == 2,]

  
  plot = ggplot(data, aes(x=factor(payment_type), y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=1, position="dodge") +
    xlab(NULL) +
    ylab("Occurrences") +
    facet_wrap( ~ year, ncol=9) +
    scale_x_discrete(breaks = c(1, 2), labels=c("CC", "Cash")) +
    scale_fill_discrete(name = "cluster")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_dispute_distr = function(data, save_name='') {
  
  data = data[data$payment_type == 4, ]
  
  plot = ggplot(data, aes(x=payment_type, y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=0.3, position="dodge") +
    facet_wrap( ~ year, ncol=5) +
    scale_fill_discrete(name = "cluster")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}


plot_clulstered_tolls_amount_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=CAST.tolls_amount.AS.INT., y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=0.3, position="stack") +
    xlim(0, 10) +
    scale_fill_discrete(name = "cluster")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_passenger_count_distr = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=passenger_count, y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=1, position="dodge") +
    xlab("Passenger #") +
    ylab("Occurrences") +
    scale_fill_discrete(name = "cluster")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_avg_tip_percentage = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=cluster, y=avg.tip_percentage.)) + 
    geom_histogram(stat="identity")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_pickup_hour_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=factor(pickup_hour), y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=1, position="dodge") +
    #geom_density(stat="identity") +
    xlab("Pickup hour") +
    ylab("Occurrences") +
    scale_fill_discrete(name = "cluster") +
    scale_x_discrete(breaks = c(0, 6, 12, 18, 23))
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_pickup_weekday_dist = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=factor(pickup_day), y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", alpha=1, position="dodge") +
    xlab("Pickup day") +
    ylab("Occurrences") +
    scale_fill_discrete(name = "cluster") +
    scale_x_discrete(labels=c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"))
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}


plot_clulstered_pickup_location_id_dist = function(data, shapefile_df, save_name='') {
  
  
  for(current_cluster in c(0, 1, 2, 3, 4)) {
    # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
    # Paths handle clipping better. Polygons can be filled.
    # You need the aesthetics long, lat, and group.
    current_data = data[data$cluster == current_cluster, ]
    
    
    pickup_location_df = current_data
    
    colnames(pickup_location_df) <- c("x", "id", "cluster", "count")
    
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
      ggsave(paste(save_name, current_cluster, '_', current_cluster, '_map.pdf', sep='')) 
    }
    print(plot)
  }
  
  plot = ggplot(data, aes(x=pickup_location_id, y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", position="stack")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_clulstered_dropoff_location_id_dist = function(data, shapefile_df, save_name='') {
  
  
  for(current_cluster in c(0, 1, 2, 3, 4)) {
    # Now the shapefile can be plotted as either a geom_path or a geom_polygon.
    # Paths handle clipping better. Polygons can be filled.
    # You need the aesthetics long, lat, and group.
    current_data = data[data$cluster == current_cluster, ]
    
    
    pickup_location_df = current_data
    
    colnames(pickup_location_df) <- c("x", "id", "cluster", "count")
    
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
      ggsave(paste(save_name, current_cluster, '_', current_cluster, '_map.pdf', sep='')) 
    }
    print(plot)
  }
  
  plot = ggplot(data, aes(x=dropoff_location_id, y=count, fill=factor(cluster))) + 
    geom_histogram(stat="identity", position="stack")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}

plot_elbow = function(data, save_name='') {
  
  plot = ggplot(data, aes(x=k, y=error)) + 
    geom_line() +
    geom_point() +
    xlab("k") +
    ylab("Clustering error")
  
  if(nchar(save_name) > 1) {
    ggsave(paste(save_name, '.pdf', sep='')) 
  }
  print(plot)
  
}
