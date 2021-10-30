from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark1 = SparkSession.builder.appName("functions").master("local").getOrCreate()
print(spark1)
spark2 = SparkSession.builder.appName("functions1").master("local[2]").getOrCreate()
print(spark2)

