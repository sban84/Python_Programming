from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# spark = SparkSession.builder.getOrCreate()
# data = [("a", 1), ("b", 2)]
#
# df = spark.createDataFrame(data, ["name", "age"])
# df.show()
#
#
# def addOne(data):

#     result = []
#     for row in data:
#         row.age += 1
#         result.append([row.name, row.age])
#
#     return iter(result)
#
#
# result_rdd1 = df.rdd.mapPartitions(lambda x: addOne(x))
# print(result_rdd1.collect())


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [('James', 'Smith', 'M', 3000),
        ('Anna', 'Rose', 'F', 4100),
        ('Robert', 'Williams', 'M', 6200),
        ]

columns = ["firstname", "lastname", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df = df.repartition(2)
df.show()
print(df.rdd.getNumPartitions())

# mapPartitions usage
def reformat2(partitionData):
    updatedData = []
    print("Inside reformat2")
    for row in partitionData:
        name = row.firstname + "," + row.lastname
        bonus = row.salary * 10 / 100
        updatedData.append([name, bonus])
    return iter(updatedData)


rdd_result = df.rdd.mapPartitions(lambda x: reformat2(x))
print(rdd_result.collect())

df2 = df.rdd.mapPartitions(reformat2).toDF(["name", "bonus"])
df2.show()


def reformat3(idx, partitionData):
    updatedData = []
    print("Inside reformat3", idx)
    for row in partitionData:
        name = row.firstname + "," + row.lastname
        bonus = row.salary * 10 / 100
        updatedData.append([name, bonus])
    return iter(updatedData)


df3 = df.rdd.mapPartitionsWithIndex(lambda idx, x: reformat3(idx, x)).toDF(["name", "bonus"])
df3.show()
