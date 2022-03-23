from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (SparkSession.builder.appName("FireCalls").getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
    StructField('UnitID', StringType(), True),
    StructField('IncidentNumber',IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', StringType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', StringType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisiorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)])

sf_fire_file = "/home/coleballew/hadoop/spark_hadoop/github_examples/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header = True, schema = fire_schema)
'''
few_fire_df = (fire_df.select("IncidentNumber", "AvailableDtTm", "CallType", "City").where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)
'''
#(fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().show(10, False))

# Creating new dataframe with renamed columns (IncidentDate, OnWatchDate, AvailableDtTS) with timestamps converted using to_timestamp

fire_ts_df = (fire_df
        .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
        .drop("CallDate")
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
        .drop("WatchDate")
        .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
        .drop("AvailableDtTm"))

#(fire_df.select("CallDate", "WatchDate", "AvailableDtTm").show(5, False))
#(fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False)) 
#(fire_ts_df.select(year("IncidentDate")).distinct().orderBy(year("IncidentDate")).show())

# What were all the different types of fire calls in 2018

(fire_ts_df.select("CallType", "IncidentDate")
        .where(year("IncidentDate") == 2018)
        .distinct()
        .show(10))

