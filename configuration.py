from pyspark.sql import SparkSession

appName = 'Parquet Converter'
master = 'local[*]'

# Create a SparkSession. No need to create SparkContext
# You automatically get it as part of the SparkSession
spark = SparkSession.builder.appName(appName).getOrCreate()
