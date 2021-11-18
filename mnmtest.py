from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("mnmExample").getOrCreate())

data = "/home/opc/spark/mnm_dataset.csv"

df = (spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data))

df.select("State", "Count").where(col("Count") > 50).show(5, truncate=False)
#df_count = (df.select("State", "Color", "Count").where(df.State == "TX").show(n=10, truncate=False))



df.printSchema()

