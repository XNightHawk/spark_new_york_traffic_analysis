import pyspark
from pyspark.sql import SparkSession

appName = 'Parquet Converter'

conf = pyspark.SparkConf()
conf.set("spark.executor.memory", '1g')
conf.set('spark.executor.cores', '1')
conf.setMaster("local[1]")

sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.appName(appName).getOrCreate()

