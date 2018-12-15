'''
Code for coordinate conversion exploration
'''

from configuration import spark
from schema_conversion import *

dataset_folder = '/media/luca/TOSHIBA EXT/BigData/datasets/'
results_folder = '/media/luca/TOSHIBA EXT/BigData/datasets/stats/'

#green_2016 = spark.read.parquet(dataset_folder + 'green_tripdata_2016.parquet')
yellow_2016 = spark.read.parquet(dataset_folder + 'yellow_tripdata_2016.parquet')

#green_2016 = v2_green_to_common(green_2016)
#green_2016.show()

yellow_2016 = v2_yellow_to_common(yellow_2016)
yellow_2016.show()

#spark.sql("select count(*) as num_of_green_2013 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/green_tripdata_2013.parquet`").show()
#spark.sql("select count(*) as num_of_green_2014 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/green_tripdata_2014.parquet`").show()
#spark.sql("select count(*) as num_of_green_2015 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/green_tripdata_2015.parquet`").show()
#spark.sql("select count(*) as num_of_green_2016 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/green_tripdata_2016.parquet`").show()
#spark.sql("select count(*) as num_of_green_2017 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/green_tripdata_2017.parquet`").show()
#spark.sql("select count(*) as num_of_green_2018 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/green_tripdata_2018.parquet`").show()

#spark.sql("select count(*) as num_of_yellow_2013 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/yellow_tripdata_2013.parquet`").show()
#spark.sql("select count(*) as num_of_yellow_2014 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/yellow_tripdata_2014.parquet`").show()
#spark.sql("select count(*) as num_of_yellow_2015 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/yellow_tripdata_2015.parquet`").show()
#spark.sql("select count(*) as num_of_yellow_2016 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/yellow_tripdata_2016.parquet`").show()
#spark.sql("select count(*) as num_of_yellow_2017 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/yellow_tripdata_2017.parquet`").show()
#spark.sql("select count(*) as num_of_yellow_2018 from parquet.`/media/luca/TOSHIBA EXT/BigData/datasets/yellow_tripdata_2018.parquet`").show()

