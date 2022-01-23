from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

l = [10, 2, 3, 4, 50, 6, 7]

rdd = spark.sparkContext.parallelize(l, 3)
print(rdd.collect())

# 1. based way to sort rdd is using zip with index
rdd_zip_with_index = rdd.zipWithIndex()
print(rdd_zip_with_index.collect())

rdd_sorted = rdd_zip_with_index.sortBy(lambda x: x[0], ascending=False)
rdd_sorted_df = spark.createDataFrame(rdd_sorted, ["number", "index"])
rdd_sorted_df.show()

# 2. reverse rdd
rdd_rev = rdd_zip_with_index.sortBy(lambda x: x[1], ascending=False)
print(rdd_rev.collect())


print(spark.sparkContext.defaultParallelism)
print(spark.sparkContext.defaultMinPartitions)
