from numpy import NaN
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank , dense_rank , row_number
from pyspark.sql.window import Window

spark =  SparkSession.builder.getOrCreate()
data = [("a","b" ,10), ("a", "", 10), ("a", "c", 20)]
df = spark.createDataFrame(data).toDF("col1", "col3" ,"col2")


windowSpec = Window.partitionBy("col1").orderBy("col2")

df_with_result  = df.withColumn("rank", rank().over(windowSpec)).withColumn("dense_rank", dense_rank().over(windowSpec))\
    .withColumn("row_number", row_number().over(windowSpec))
df_with_result.show()

