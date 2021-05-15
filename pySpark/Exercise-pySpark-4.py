from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import   *
import pandas as pd

spark = SparkSession.builder.appName("Test-4").master("local").getOrCreate()
spark_1 = SparkSession.builder.appName("Test").master("local").getOrCreate()
print(spark)
print(spark_1) # same sparksession object will be returned by builder pattern.

df = spark.createDataFrame([
(1, 144.5, 5.9, 33, 'M'), (2, 167.2, 5.4, 45, 'M'), (3, 124.1, 5.2, 23, 'F'),
    (4, 144.5, 5.9, 33, 'M'), (5, 133.2, 5.7, 54, 'F'), (3, 124.1, 5.2, 23, 'F'),
    (5, 129.2, 5.3, 42, 'M'), ],
    ['id', 'weight', 'height','age', 'gender'])

df.show()

# Cleaning the DATA , NOTE Very important code , useful code
c =  df.count()
c_distinct = df.distinct().count()
print(f" total rec in DF = {c} and distinct = {c_distinct}")

