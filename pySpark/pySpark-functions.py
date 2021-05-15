from pyspark.sql.functions import first
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("functions").master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.createDataFrame([(2,100), (5,200), (5,300)], ('age',"salary"))
df.show()
df.agg(collect_list('age').alias("age")).show()

df.printSchema()
df.groupby(col("age")).agg(collect_set(col("salary"))).show()



#2. element_at for list and map , it os diff in these 2 of accessing
from pyspark.sql import Row
eDF = spark.createDataFrame([
    (Row(1, [1,2,3], {"India": "Delhi"})),
    (Row(2, [11,21,31],{"Germany": "Berlin"}))
                            ] , ["id" , "scores" ,"country" ])
eDF.show()
eDF.printSchema()

eDF.withColumn("scores_exploded" , explode(col("scores"))).show()

eDF.select(explode(col("country")).alias("key", "value")).show()
eDF.filter(element_at(col("country") , lit("India")) == "Delhi").show()

eDF.select(element_at(col("scores") , 1)).show()