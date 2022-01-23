from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

appName = "PySpark Partition Example"
master = "local[8]"

# Create Spark session with Hive supported.
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

print(spark.version)
# Populate sample data
start_date = date(2019, 1, 1)
data = []
for i in range(0, 50):
    data.append({"Country": "CN", "Date": start_date +
                                          relativedelta(days=i), "Amount": 10 + i})
    data.append({"Country": "AU", "Date": start_date +
                                          relativedelta(days=i), "Amount": 10 + i})

schema = StructType([StructField('Country', StringType(), nullable=False),
                     StructField('Date', DateType(), nullable=False),
                     StructField('Amount', IntegerType(), nullable=False)])

df = spark.createDataFrame(data, schema=schema)
df.show()

df.withColumn("partition_id", spark_partition_id()).groupby(col("partition_id")) \
    .agg(collect_list(col("Country"))).show(truncate=False)

# 1. saving as it is, no of files = no of partitions
print(df.rdd.getNumPartitions())
df.write.mode("overwrite").csv("./output_1/example.csv", header=True)

# 2. partition by col country while saving, it will create 2 folder 1 for
# each country ( we have only CN and AU)
# and 8 files
df.write.mode("overwrite").partitionBy("Country").csv("./output_2/example.csv", header=True)

# 3. repartition in Spark side ( in memory partitions, used normally for performance boost)
df = df.repartition(col("Country"))
print(df.rdd.getNumPartitions())  # will be default 200 partitions
# will create 1 files for each Partition col one for CN and one for AU
# as we used df.repartition(col("country")) even though it will have 200 partitions in spark memory
# while save it will see that all other partitions are empty so it be ignored
df.write.mode("overwrite").partitionBy("Country").csv("./output_3/example.csv", header=True)

# 4. repartition in Spark side
print("------Repartition by 4 -------")
df = df.repartition(4, col("Country"))
print(df.rdd.getNumPartitions())  # will be default 4 partitions

# now while save without partition it will create 4 files only as we have 4 partitions in memory
# and we are not doing any partitionBy while save , so same as case 1.
# empty files will be ignored if any while save
df.write.mode("overwrite").csv("./output_4/example.csv", header=True)
df.withColumn("partition_id", spark_partition_id()).groupby(col("partition_id")) \
    .agg(collect_list(col("Country"))).show(truncate=False)

# this is same as case 3.
df.write.mode("overwrite").partitionBy("Country").csv("./output_5/example.csv", header=True)
