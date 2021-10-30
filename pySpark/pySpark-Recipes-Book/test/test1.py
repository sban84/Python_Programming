from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

lines = """This is sample program to see how this sample works"""
list_split = lines.split(" ")
spark = SparkSession.builder.appName("test").master("local").getOrCreate()

rdd = spark.sparkContext.parallelize(list_split)

df = spark.createDataFrame(rdd, StringType()).withColumnRenamed("value","words")
df.show()

df = df.withColumn("w_length" , length(col("words")))
df.show(truncate=False)
