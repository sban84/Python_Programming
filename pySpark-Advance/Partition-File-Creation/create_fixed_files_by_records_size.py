from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, split, array, struct
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("array_type_example").master("local") \
    .config("spark.driver.bindAddress", "localhost") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [
    ("A0", "China"),
    ("A1", "China"),
    ("A2", "China"),
    ("A3", "China"),
    ("A4", "China"),
    ("A5", "China"),
    ("A6", "China"),
    ("B1", "France"),
    ("B1", "France"),
    ("B3", "France"),
    ("C6", "Cuba"),
]

df = spark.createDataFrame(data, ["name", "country"])
df.show()
print(df.rdd.getNumPartitions())
# df.selectExpr("distinct(count('country')) as cnt")
df_country_count = df.groupby("country").count()

df_with_country_count = df.join(df_country_count, ["country"]
                                )
df_with_country_count.show(truncate=False)

df_with_country_count = df_with_country_count.withColumn("repart_seed",
                                                         (rand() * df_with_country_count["count"] / 4).cast(IntegerType()))
df_with_country_count.show(truncate=False)

repart_cols = ["country"]
df_with_country_count = df_with_country_count.repartition('repart_seed',"country")
# df = df.repartition(4, "country")
# print(df.rdd.getNumPartitions())
df_with_country_count.write.mode("overwrite").option("header","true").format("csv").partitionBy("country").save("./target/")
