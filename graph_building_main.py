'''
Creates a network graph of the taxi zones
'''

import builtins
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

dataset_folder = '/media/luca/TOSHIBA EXT/BigData/datasets/'
results_folder = '/media/luca/TOSHIBA EXT/BigData/datasets/stats/'

max_rank = 30
min_rank = 3


class NodeAttribute:

    def __init__(self, borough, zone):
        self.borough = borough
        self.zone = zone


def get_threshold(normalized_count, max_rank, min_rank):
    threshold = builtins.round(normalized_count * max_rank)
    if threshold < min_rank:
        threshold = min_rank
    return threshold


taxi_zone_in = pd.read_csv('file://' + dataset_folder + 'taxi_zone_lookup.csv')

# List of location ids
taxi_zones_id = list(row.LocationID for row in taxi_zone_in.itertuples())

attrs = list((row.LocationID, vars(NodeAttribute(row.Borough, row.Zone))) for row in taxi_zone_in.itertuples())
# Dictionary in which to each pickup_location_id, which acts as a key, is associated its borough and zone
attrs = {key: value for (key, value) in attrs}

pickup_location_id_dist = pd.read_csv('file://' + results_folder + 'pickup_location_id_dist.csv')
# Normalization of the count column
pickup_location_id_dist['count'] = (pickup_location_id_dist['count']-pickup_location_id_dist['count'].min())/\
                                   (pickup_location_id_dist['count'].max() - pickup_location_id_dist['count'].min())

# List in which each pickup_location_id, is associated the max number of edges
thresholds = list((row.pickup_location_id, get_threshold(row.count, max_rank, min_rank))
                  for row in pickup_location_id_dist.itertuples())

clustered_dataset = pd.read_csv('file://' + results_folder + 'pickup_location_and_dropoff_location_distribution.csv')
# Drop index column
clustered_dataset = clustered_dataset.drop(clustered_dataset.columns[0], axis=1)

# Dataset containing trips that will be represented in the network graph
dataset = None

for pickup_location_id, threshold in thresholds:
    current_dataset = clustered_dataset.query('pickup_location_id == @pickup_location_id & rank <= @threshold')

    if dataset is None:
        dataset = current_dataset
    else:
        dataset = dataset.append(current_dataset, ignore_index=True)

# Creates a multi graph
G = nx.MultiGraph()
G.add_nodes_from(taxi_zones_id)
nx.set_node_attributes(G, attrs)
for row in dataset.itertuples():
    G.add_edge(row.pickup_location_id, row.dropoff_location_id, key=row.cluster, weight=row.count, cluster=row.cluster)

# Saving graph in a format readable by Cytoscape
nx.write_graphml(G, dataset_folder + 'traffic_graph.graphml')

