'''

  ________    ______   ______     _          ____        __
 /_  __/ /   / ____/  /_  __/____(_)___     / __ \____ _/ /_____ _
  / / / /   / /        / / / ___/ / __ \   / / / / __ `/ __/ __ `/
 / / / /___/ /___     / / / /  / / /_/ /  / /_/ / /_/ / /_/ /_/ /
/_/ /_____/\____/    /_/ /_/  /_/ .___/  /_____/\__,_/\__/\__,_/
                               /_/


Authors: Willi Menapace <willi.menapace@studenti.unitn.it>
         Luca Zanella <luca.zanella-3@studenti.unitn.it>
         Daniele Giuliani <daniele.giuliani@studenti.unitn.it>

Some utility routines for computing commonly used spark dataframe columns
'''

import pyspark
import pyspark.sql.functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

import schema_conversion
from schema import *

def trip_duration_minutes_column(dataset, alias_name = "trip_duration_minutes"):
    return ((unix_timestamp(dataset[dropoff_datetime_property]) / 60).cast(IntegerType()) - (unix_timestamp(dataset[pickup_datetime_property]) / 60).cast(IntegerType())).alias(alias_name)

def speed_column(dataset, alias_name = "average_speed"):
    return (dataset[trip_distance_property] / ((unix_timestamp(dropoff_datetime_property) - unix_timestamp(pickup_datetime_property)) / 3600)).alias(alias_name)