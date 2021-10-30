from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("functions").master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Difference between DSL way of using sql functions and API way of using .
# refer https://towardsdatascience.com/a-decent-guide-to-dataframes-in-spark-3-0-for-beginners-dcc2903345a5
l = [('abcd', 2), ('abcd', 1)]
df = spark.createDataFrame(l, ['str', 'len'])

df.select("*", substring('str',0,3).alias("substr_col")).show()  # The DSL substring function doesn’t allow you to do it — the position and length need to be constant, the same on each row.

df.withColumn("substr_col", col("str").substr(lit(0), col("len"))).show(3, truncate=False)
df.selectExpr("*", "substring(str , 0,len) as substr_col").show(3, truncate=False)
# use selectExpr when we wanted to do sql way of data query / analysis , using complex functions
# just like sql , use "" and inside that write same as sql query and also allows access other col such as len here

