from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df1 = spark.range(2, 10000000, 2)
df1.show()

df2 = spark.range(2, 10000000, 4)
step1 = df1.repartition(5)
step12 = df2.repartition(6)
step2 = step1.selectExpr("id * 5 as id")
step3 = step2.join(step12, ["id"])
step4 = step3.selectExpr("sum(id)")
step4.show()
step4.explain()
time.sleep(300)
