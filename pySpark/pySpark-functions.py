from pyspark.sql.functions import first
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("functions").master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.createDataFrame([(2, 100), (5, 200), (5, 300)], ('age', "salary"))
df.show()
df.agg(collect_list('age').alias("age")).show()

df.printSchema()
df.groupby(col("age")).agg(collect_set(col("salary"))).show()

# 2. element_at for list and map , it os diff in these 2 of accessing
from pyspark.sql import Row

eDF = spark.createDataFrame([
    (Row(1, [1, 2, 3], {"India": "Delhi"})),
    (Row(2, [11, 21, 31], {"Germany": "Berlin"}))
], ["id", "scores", "country"])
eDF.show()
eDF.printSchema()

eDF.withColumn("scores_exploded", explode(col("scores"))).show()

eDF.select(explode(col("country")).alias("key", "value")).show()
eDF.filter(element_at(col("country"), lit("India")) == "Delhi").show()

eDF.select(element_at(col("scores"), 1)).show()


# Difference between DSL way of using sql functions and API way of using .
# refer https://towardsdatascience.com/a-decent-guide-to-dataframes-in-spark-3-0-for-beginners-dcc2903345a5
l = [('abcd', 2), ('abcd', 1)]
df = spark.createDataFrame(l, ['str', 'len'])

df.select("*", substring('str',0,3).alias("substr_col")).show()  # The DSL substring function doesn’t allow you to do it — the position and length need to be constant, the same on each row.

df.withColumn("substr_col", col("str").substr(lit(0), col("len"))).show(3, truncate=False)
df.selectExpr("*", "substring(str , 0,len) as substr_col").show(3, truncate=False)
# use selectExpr when we wanted to do sql way of data query / analysis , using complex functions
# just like sql , use "" and inside that write same as sql query and also allows access other col such as len here
