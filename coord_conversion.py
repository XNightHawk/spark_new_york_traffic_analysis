'''
Code for coordinate conversion exploration
'''
import schema_conversion

from schema_conversion import *

print(schema_conversion.matrix)

for i in range(schema_conversion.num_tiles):
    for j in range(schema_conversion.num_tiles):
        if schema_conversion.matrix[i][j]:
            print(str(len(schema_conversion.matrix[i][j])))
        else:
            pass
            #print(str(0))

'''
dataset_folder = '/media/luca/TOSHIBA EXT/BigData/datasets/'
results_folder = '/media/luca/TOSHIBA EXT/BigData/datasets/stats/'

green_2015 = spark.read.parquet(dataset_folder + 'green_tripdata_2015.parquet')
green_2015 = v2_green_to_common(green_2015)
green_2015.write.save(results_folder + 'green_2015.parquet')

#green_2016 = spark.read.parquet(dataset_folder + 'green_tripdata_2016.parquet')
#green_2016 = v2_green_to_common(green_2016)
#green_2016.write.save(results_folder + 'green_2016.parquet')

#yellow_2016 = spark.read.parquet(dataset_folder + 'yellow_tripdata_2016.parquet')
#yellow_2016 = v2_yellow_to_common(yellow_2016)
#yellow_2016.write.save(results_folder + 'yellow_tripdata_2016.parquet')

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

'''