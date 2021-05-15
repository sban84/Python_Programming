## This program is good to refer ow to create manual data
## and then create RDD and DataFrame from there
## We can create DF directly also shown here....
## by providing schema while creating DF

from pyspark import  SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pandas as pd


spark = SparkSession.builder.appName("Test-2").master("local").getOrCreate()
#sc = spark.sparkContext()
rdd = spark.sparkContext.parallelize( [1,2,3,4,5,6] )
print(rdd.collect())

# create DF from the RDD
df = spark.createDataFrame(rdd , IntegerType())
df.show()


dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)
df = rdd.toDF(["dept" , "sal"])
df.show(truncate=False)


## provide Schema manually to create DF
schema = StructType([StructField("name",StringType(),True), StructField("id",IntegerType(),True)])
df = spark.createDataFrame(pd.DataFrame( [ ["a",1] , ["b", 2] ] ) , schema=schema )
df.show(truncate=False)

list_data = [ ("a",1),("b" , 10)]  # its same if we have list of list / list of tuple
df = spark.createDataFrame(list_data , schema=schema)
df.show(truncate=False)