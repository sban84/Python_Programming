from pyspark.sql import SparkSession
from pyspark.sql.functions import *

"""
Good code to understand glom and mapPartitions and foreachPartitions

df.rdd.glom() normally returns data in list taking all the data for a Partitions
e.g. if we have 4 partitions , so it will create separate list for each Partition 
and will return as list[list[Row]] like [[Row(id=1, name='a', sal=100), Row(id=2, name='b', sal=200)], []]
but in collect everything will be in one list like 
[Row(id=1, name='a', sal=100), Row(id=2, name='b', sal=200)]
"""
spark = SparkSession.builder.appName("glom_test").getOrCreate()

data = [(1, "a", 100),
        (2, "b", 200)]

df = spark.createDataFrame(data, ["id", "name", "sal"])

df.show(truncate=False)
print(f"no. of partition {df.rdd.getNumPartitions()}")
print(df.rdd.glom().collect())
df = df.repartition(2)
print(f"using glom {df.rdd.glom().collect()}")
print(f"using collect only {df.rdd.collect()}")

for row in df.rdd.collect():
    if (row is not None) & (len(row) > 0):
        print(row["id"])

# iterating if we use glom
for rows in df.rdd.glom().collect():
    for row in rows:
        if (row is not None) & (len(row) > 0):
            print(row["id"])


# NOTE :: this below code just to example how to use mapPartitions which process data on partition basis
# (partitions together.)
#  and the function called need to yield  result as here we are returning list
# the difference from foreachPartition is the called function does not need to return
# just process records of each partitions together.
def print_iterator(itr):
    print("inside print_iterator")
    for row in itr:
        yield ["new_" + str(row["id"]) , row["name"], row["sal"]] # no return obviously :)


rdd2 = df.rdd.mapPartitions(lambda itr: print_iterator(itr))
df2 = spark.createDataFrame(rdd2, ["new_id" , "name", "sal"])
df2.show()
