'''

  ________    ______   ______     _          ____        __
 /_  __/ /   / ____/  /_  __/____(_)___     / __ \____ _/ /_____ _
  / / / /   / /        / / / ___/ / __ \   / / / / __ `/ __/ __ `/
 / / / /___/ /___     / / / /  / / /_/ /  / /_/ / /_/ / /_/ /_/ /
/_/ /_____/\____/    /_/ /_/  /_/ .___/  /_____/\__,_/\__/\__,_/
                               /_/


Authors: Willi Menapace <willi.menapace@studenti.unitn.it>
         Luca Zanells <luca.zanella-3@studenti.unitn.it>

Creates a network graph of the taxi zones

'''

import builtins
from collections import defaultdict
import pyspark
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.functions import *
import csv
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from schema import *

dataset_folder = '/media/luca/TOSHIBA EXT/BigData/datasets/'
results_folder = '/media/luca/TOSHIBA EXT/BigData/datasets/stats/'

location_id_property = 'LocationID'
borough_property = 'Borough'
zone_property = 'Zone'
count_property = 'count'
avg_distance_property = 'avg(trip_distance)'
avg_duration_property = 'avg(duration_seconds)'
avg_speed_property = 'avg(avg_speed)'
avg_total_amount_property = 'avg(total_amount)'

max_rank = 30
min_rank = 3

low_level = 'LOW'
medium_level = 'MEDIUM'
high_level = 'HIGH'
activity_level = 'activity_level'
distance_level = 'distance_level'
duration_level = 'duration_level'
speed_level = 'speed_level'
total_amount_level = 'total_amount_level'


def nested_dict():
    return defaultdict(nested_dict)

def compute_location_id_borough_and_zone(dataset, attrs):
    for index, row in dataset.iterrows():
        curr_pickup_location_id = row[location_id_property]
        curr_borough = row[borough_property]
        curr_zone = row[zone_property]

        attrs[curr_pickup_location_id]['location_id'] = curr_pickup_location_id
        attrs[curr_pickup_location_id][borough_property.lower()] = curr_borough
        attrs[curr_pickup_location_id][zone_property.lower()] = curr_zone

def compute_activity_level(dataset, attrs):
    first_quartile = dataset[count_property].quantile(0.25)
    third_quartile = dataset[count_property].quantile(0.75)

    for index, row in dataset.iterrows():
        curr_pickup_location_id = row[pickup_location_id_property]
        curr_count = row[count_property]

        if curr_count < first_quartile:
            attrs[curr_pickup_location_id][activity_level] = low_level
        elif curr_count < third_quartile:
            attrs[curr_pickup_location_id][activity_level] = medium_level
        else:
            attrs[curr_pickup_location_id][activity_level] = high_level

def compute_distance_level(dataset, attrs):
    first_quartile = dataset[avg_distance_property].quantile(0.25)
    third_quartile = dataset[avg_distance_property].quantile(0.75)

    for index, row in dataset.iterrows():
        curr_pickup_location_id = row[pickup_location_id_property]
        curr_avg_distance = row[avg_distance_property]

        if curr_avg_distance < first_quartile:
            attrs[curr_pickup_location_id][distance_level] = low_level
        elif curr_avg_distance < third_quartile:
            attrs[curr_pickup_location_id][distance_level] = medium_level
        else:
            attrs[curr_pickup_location_id][distance_level] = high_level

def compute_duration_level(dataset, attrs):
    first_quartile = dataset[avg_duration_property].quantile(0.25)
    third_quartile = dataset[avg_duration_property].quantile(0.75)

    for index, row in dataset.iterrows():
        curr_pickup_location_id = row[pickup_location_id_property]
        curr_avg_duration = row[avg_duration_property]

        if curr_avg_duration < first_quartile:
            attrs[curr_pickup_location_id][duration_level] = low_level
        elif curr_avg_duration < third_quartile:
            attrs[curr_pickup_location_id][duration_level] = medium_level
        else:
            attrs[curr_pickup_location_id][duration_level] = high_level

def compute_speed_level(dataset, attrs):
    first_quartile = dataset[avg_speed_property].quantile(0.25)
    third_quartile = dataset[avg_speed_property].quantile(0.75)

    for index, row in dataset.iterrows():
        curr_pickup_location_id = row[pickup_location_id_property]
        curr_avg_speed = row[avg_speed_property]

        if curr_avg_speed < first_quartile:
            attrs[curr_pickup_location_id][speed_level] = low_level
        elif curr_avg_speed < third_quartile:
            attrs[curr_pickup_location_id][speed_level] = medium_level
        else:
            attrs[curr_pickup_location_id][speed_level] = high_level

def compute_total_amount_level(dataset, attrs):
    first_quartile = dataset[avg_total_amount_property].quantile(0.25)
    third_quartile = dataset[avg_total_amount_property].quantile(0.75)

    for index, row in dataset.iterrows():
        curr_pickup_location_id = row[pickup_location_id_property]
        curr_avg_total_amount = row[avg_total_amount_property]

        if curr_avg_total_amount < first_quartile:
            attrs[curr_pickup_location_id][total_amount_level] = low_level
        elif curr_avg_total_amount < third_quartile:
            attrs[curr_pickup_location_id][total_amount_level] = medium_level
        else:
            attrs[curr_pickup_location_id][total_amount_level] = high_level

def get_rank_thresholds(dataset, max_rank, min_rank):
    thresholds = list()

    # Normalization of the count column
    dataset[count_property] = (dataset[count_property] - dataset[count_property].min()) /\
                              (dataset[count_property].max() - dataset[count_property].min())

    for index, row in dataset.iterrows():
        curr_pickup_location_id = row[pickup_location_id_property]
        curr_count = row[count_property]

        curr_threshold = builtins.round(curr_count * max_rank)
        if curr_threshold < min_rank:
            curr_threshold = min_rank

        thresholds.append((curr_pickup_location_id, curr_threshold))

    return thresholds


taxi_zone_lookup = pd.read_csv('file://' + dataset_folder + 'taxi_zone_lookup.csv')
pickup_location_id_dist = pd.read_csv('file://' + results_folder + 'pickup_location_id_dist.csv')
avg_distance_by_pickup_location = pd.read_csv('file://' + results_folder + 'avg_distance_by_pickup_location.csv')
avg_duration_by_pickup_location = pd.read_csv('file://' + results_folder + 'avg_duration_by_pickup_location.csv')
avg_speed_by_pickup_location = pd.read_csv('file://' + results_folder + 'avg_speed_by_pickup_location.csv')
avg_total_amount_by_pickup_location = pd.read_csv('file://' + results_folder + 'avg_total_amount_by_pickup_location.csv')
rank_by_pickup_location_and_dropoff_location = pd.read_csv('file://' + results_folder + 'rank_by_pickup_location_and_dropoff_location.csv')

# List of location ids
taxi_zones_id = list(row[location_id_property] for index, row in taxi_zone_lookup.iterrows())

# Nested dictionary containing attributes for each location id
attrs = nested_dict()

compute_location_id_borough_and_zone(taxi_zone_lookup, attrs)
compute_activity_level(pickup_location_id_dist, attrs)
compute_distance_level(avg_distance_by_pickup_location, attrs)
compute_duration_level(avg_duration_by_pickup_location, attrs)
compute_speed_level(avg_speed_by_pickup_location, attrs)
compute_total_amount_level(avg_total_amount_by_pickup_location, attrs)

# List in which each pickup_location_id is associated to its max number of edges
thresholds = get_rank_thresholds(pickup_location_id_dist, max_rank, min_rank)

# Drop index column
rank_by_pickup_location_and_dropoff_location = rank_by_pickup_location_and_dropoff_location\
    .drop(rank_by_pickup_location_and_dropoff_location.columns[0], axis=1)

# Dataset containing trips that will be represented in the network graph
dataset = None

for pickup_location_id, threshold in thresholds:
    current_dataset = rank_by_pickup_location_and_dropoff_location.query('pickup_location_id == @pickup_location_id & rank <= @threshold')

    if dataset is None:
        dataset = current_dataset
    else:
        dataset = dataset.append(current_dataset, ignore_index=True)

# Creates a multi graph
G = nx.MultiGraph()
G.add_nodes_from(taxi_zones_id)
nx.set_node_attributes(G, attrs)
for index, row in dataset.iterrows():
    G.add_edge(row[pickup_location_id_property], row[dropoff_location_id_property], key=row[clustering_class_property],
               weight=row[count_property], cluster=row[clustering_class_property])

# Saving graph in a format readable by Cytoscape
nx.write_graphml(G, dataset_folder + 'traffic_graph.graphml')



