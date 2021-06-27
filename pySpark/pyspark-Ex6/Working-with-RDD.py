"""
Good exmaple code to brush up RDD and DF creations
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField
from pyspark.sql.functions import *
import time

spark = SparkSession.builder.appName("RDD-Test").master("local").getOrCreate()


data = [1,2,3,4,5]

# 1. Sum

rdd =  spark.sparkContext.parallelize(data)
print(rdd.collect())
sum_way1 = rdd.sum()
print("sum_way1" , sum_way1)

sum_way2 = rdd.reduce(lambda x,y: x+y)
print("sum_way2" , sum_way2)

# 2. create paired RDD
data = [("a" , "1"),("a", "2"),('b', "3")] # list of list is same , here used list of tuples
rdd = spark.sparkContext.parallelize(data,4)
print(rdd.getNumPartitions())

start = time.time()
words_counts = rdd.map(lambda x: (x[0],int(x[1]))).reduceByKey(lambda x,y : x+y)
print(words_counts.collect())

print(f"time taken {time.time() - start}")

# Using Dataframe

# preferred way to create a DF from data, createDataFrame can take rdd , raw data list of list / tuples
#
df = spark.createDataFrame(data , ["name" , "score"] )
df.show()
# df = spark.createDataFrame(rdd , ["name" , "score"] )
# df.show()

schema =  StructType([StructField("name",StringType(),False) , StructField("score",StringType() , True) ])
df =  spark.createDataFrame(data,schema)
df.select(col("name"),col("score").cast(IntegerType())).show()