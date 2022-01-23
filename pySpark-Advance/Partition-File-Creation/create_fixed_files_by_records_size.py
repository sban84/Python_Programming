"""
Very important code
Ways to create approx size files using repartition()
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, split, array, struct
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("fixed_no_records_each_file").master("local") \
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
    ("A7", "China"),
    ("A8", "China"),
    ("B1", "France"),
    ("B1", "France"),
    ("B3", "France"),
    ("C6", "Cuba"),
]

df = spark.createDataFrame(data, ["name", "country"])
df.show()
# case 1: when we did not use repartition, so generated files will have 3 files even though
# before save we have 1 partition in spark memory ,
# in the storage file system 3 files will be created for respective country data
# because of partitionBy("country") below
print("\n case 1**************")
print(f"original data partitions {df.rdd.getNumPartitions()}")
df.write.mode("overwrite").option("header", "true").format("csv").partitionBy("country").save(
    "./target_without_sol/")

# case 2: df.repartition(num_partition) will divide the data into almost equal
# size partitions but there is no guarantee of num of
# records and similarity of the data in each partition.
print("\n case 2**************")
df = df.repartition(4)  # by default its 200 Partition
print(f"after repartition by 4 {df.rdd.getNumPartitions()}")
df = df.withColumn("partitionId", spark_partition_id())
df.show(truncate=False)
df.write.mode("overwrite").option("header", "true").format("csv").save(
    "./target_with_fixed_num_and_partitionBy_save/")

# case 3: repartition by fixed number, so 4 partition in spark memory and 4 files
# will be generated if we dont use partitioBy() while save
# if we use partitioBy(country) then we will generate to 4 files for each country
# if the num of records for the country is more than 4 else less no of files will be generated.
# e.g. if we use repartition(2) , then for china 2 files , France 2 files and for cuba 1 files
#
# NOTE: So this could be one solution to merge/ avoid small files issue
# if we if know that we have 100 records for a col and we know 50 records makes 100-128 MB size in HDFS
# then we can use repartition(2), but there is no guarantee that all the time ,
# same country's data will go in diff partition, if that does not happen then
# all similar data will go in one partition and will create single files for that
print("\n case 3**************")
df = df.repartition(2)
print(f"\nafter repartition by 2 only  {df.rdd.getNumPartitions()}")
df = df.withColumn("partitionId", spark_partition_id())
df.show(truncate=False)
df.write.mode("overwrite").option("header", "true").format("csv").partitionBy("country").save(
    "./target_with_fixed_num_repartition/")

print("\n ********* Fixed records ( avoid small file issue in HDFS) ***** ")
# case 4: Now to the problem of distributing approx equal no of records in each file
# refer this solution.
# df.selectExpr("distinct(count('country')) as cnt")
print("\n case 4**************")
df_country_count = df.groupby("country").count()

df_with_country_count = df.join(df_country_count, ["country"])
# df_with_country_count = df.join(df_country_count, df.country == df_country_count.country)

print("\n df_with_country_count\n")
df_with_country_count.show(truncate=False)

# NOTE : the logic of doing rand() * df_with_country_count["count"] / 4 is
# rand() will generate random no between 0 and 1
# when multiplied by (df_with_country_count["count"] / no of records per file )
# will generate numbers repart_seed between 0 and (df_with_country_count["count"] / no of records per file ) -1
# so finally each record of China will have repart_seed between 0 and one less than above result.
#
# But we need keep in mind that no_of_recods_per_file_approx need to be selected by
# experimenting the data size it takes ( normally 100 to 128 MB)
no_of_recods_per_file_approx = 4
df_with_country_count = df_with_country_count.withColumn("repart_seed",
                                                         (rand() * (df_with_country_count[
                                                                        "count"] / no_of_recods_per_file_approx)).cast(
                                                             IntegerType()))
print("\n **** df_with_country_count **** \n")
df_with_country_count.show(truncate=False)

repart_cols = ["country"]
df_with_country_count = df_with_country_count.repartition('repart_seed', 'country')
print(df_with_country_count.rdd.getNumPartitions())
# df = df.repartition(4, "country")
# print(df.rdd.getNumPartitions())
df_with_country_count.write.mode("overwrite").option("header", "true").format("csv").partitionBy("country").save(
    "./target/")
