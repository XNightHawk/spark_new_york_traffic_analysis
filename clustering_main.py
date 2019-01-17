'''
Statistical data analysis routines
'''

import pyspark
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler, OneHotEncoderEstimator, SQLTransformer

import schema_conversion
from schema import *
from computed_columns import *
from statistics import *

taxi_company_indexed_property = 'taxi_company_indexed'
pickup_hour_property = "pickup_hour"
dropoff_hour_property = "dropoff_hour"
weekend_property = 'weekend'
pickup_hour_encoded_property = pickup_hour_property + '_encoded'
dropoff_hour_encoded_property = dropoff_hour_property + '_encoded'
taxi_company_encoded_property = taxi_company_indexed_property + '_encoded'
ratecode_id_encoded_property = ratecode_id_property + '_encoded'
payment_type_encoded_property = payment_type_property + '_encoded'
unscaled_vector_property = "unscaled_features_vector"
scaled_vector_property = "scaled_features_vector"
partial_clustering_features_property = 'partial_features'
clustering_features_property = 'clustering_features'

def cluster(dataset, max_clusters = 5, max_iterations = 40, clustering_prediction_property = "clustering_predictions"):

    taxi_company_indexer = StringIndexer(inputCol=taxi_company_property, outputCol=taxi_company_indexed_property)

    pickup_hour_extractor = SQLTransformer(statement = "SELECT *, HOUR(" + pickup_datetime_property + ") AS " + pickup_hour_property + " FROM __THIS__")
    dropoff_hour_extractor = SQLTransformer(statement = "SELECT *, HOUR(" + dropoff_datetime_property + ") AS " + dropoff_hour_property + " FROM __THIS__")
    weekend_extractor = SQLTransformer(statement = "SELECT *, (DAYOFWEEK(" + pickup_datetime_property + ") == 6 OR DAYOFWEEK(" + pickup_datetime_property + ") == 5) AS " + weekend_property + " FROM __THIS__")

    one_hot_encoder = OneHotEncoderEstimator(inputCols=[taxi_company_indexed_property, ratecode_id_property, payment_type_property, pickup_hour_property, dropoff_hour_property], outputCols=[taxi_company_encoded_property, ratecode_id_encoded_property, payment_type_encoded_property, pickup_hour_encoded_property, dropoff_hour_encoded_property], handleInvalid='keep')
    vector_assembler = VectorAssembler(inputCols=[taxi_company_indexed_property, passenger_count_property, trip_distance_property, ratecode_id_encoded_property, fare_amount_property, tolls_amount_property, payment_type_encoded_property, weekend_property], outputCol=partial_clustering_features_property)

    unscaled_vector_assembler = VectorAssembler(inputCols=[passenger_count_property, trip_distance_property, fare_amount_property, tolls_amount_property], outputCol=unscaled_vector_property)
    scaler = StandardScaler(inputCol=unscaled_vector_property, outputCol=scaled_vector_property, withStd=True, withMean=True)

    complete_vector_assembler = VectorAssembler(inputCols=[partial_clustering_features_property, scaled_vector_property], outputCol=clustering_features_property)

    kmeans = KMeans(featuresCol=clustering_features_property, predictionCol=clustering_prediction_property, k=max_clusters, maxIter=max_iterations)

    pipeline = Pipeline(stages=[pickup_hour_extractor, dropoff_hour_extractor, weekend_extractor, taxi_company_indexer, one_hot_encoder, unscaled_vector_assembler, scaler, vector_assembler, complete_vector_assembler, kmeans])

    model = pipeline.fit(dataset)

    return model

def compute_k_elbow(dataset, k_from=3, k_to=10, step_size=2, training_fraction=0.001):

    k_results = {}

    for k in range(k_from, k_to, step_size):

        print("Setting k=" + str(k))

        training_dataset = dataset.sample(training_fraction)

        print("Building clustering Model")
        clustering_model = cluster(training_dataset, max_clusters=k)
        print("Computing model cost")

        featured_dataset = dataset
        kmeans_stage = 9
        for i in range(kmeans_stage):
            featured_dataset = clustering_model.stages[i].transform(featured_dataset)

        current_result = clustering_model.stages[kmeans_stage].computeCost(featured_dataset)
        print("Current cost " + str(current_result))
        k_results[k] = current_result

    return k_results

appName = 'Parquet Converter'
master = 'local[7]'

sc = pyspark.SparkContext()
spark = SparkSession.builder.appName(appName).getOrCreate()


dataset_folder = '/home/bigdata/auxiliary/'
results_folder = '/home/bigdata/auxiliary/stats/'

#Whether to use the cleaned dataset or the uncleaned one
clean_dataset = True

#Build an entry for each archive to treat attaching the relative schema conversion routine to each one
archives = []
for year in range(2010, 2019):
    if year <= 2014:
        if year >= 2013:
            archives += ['green_tripdata_' + str(year)]
        archives += ['yellow_tripdata_' + str(year)]

    elif year <= 2016:
        archives += ['green_tripdata_' + str(year)]
        archives += ['yellow_tripdata_' + str(year)]

    else:
        archives += ['green_tripdata_' + str(year)]
        archives += ['yellow_tripdata_' + str(year)]

dataset = None

if not clean_dataset:
    #Open and convert each archive to parquet format
    for archive in archives:
        print("Reading: " + archive)

        current_dataset = spark.read.parquet('file://' + dataset_folder + archive + '_common.parquet')
        if dataset is None:
            dataset = current_dataset
        else:
            dataset = dataset.union(current_dataset)

else:
    dataset = spark.read.parquet('file://' + dataset_folder + 'clean_dataset.parquet')

#dataset.printSchema()
#dataset.show()

print(compute_k_elbow(dataset, k_from=2, k_to=20, step_size=1, training_fraction=0.03))
