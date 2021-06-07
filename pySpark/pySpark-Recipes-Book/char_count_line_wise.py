"""
Counting the Number of Characters on Each Line
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("pySpark Recipes").master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.text("./words.txt").toDF("words")
df.show()
df.withColumn("char_count" , F.length(F.col("words")).cast(IntegerType())).show()
df = df.withColumn("char_count" , F.length(F.col("words")).cast(IntegerType()))
df.printSchema()
df.agg(F.sum("char_count")).show()

df.printSchema()
df.select(F.col("words")).write.mode("Overwrite").text("char_count_line_wise.txt")
df.write.mode("overwrite").option("header" , "true").csv("char_count_line_wise.csv")



