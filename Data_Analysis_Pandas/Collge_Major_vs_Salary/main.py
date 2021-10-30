from pyspark.sql import SparkSession

# pd.set_option('display.width', 400)
# pd.set_option('display.max_columns', 12)

spark = SparkSession.builder.appName("Test").master("local") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

data = spark.read.csv("salaries_by_college_major.csv")

data.show(5)
data = data.repartition(10)
print("getNumPartitions",
      data.rdd.getNumPartitions())  # NOTE : spark.default.parallelism configuration default value set to the number of all cores on all nodes in a cluster, on local it is set to a number of cores on your system.
