#Pyspark: Split multiple array columns into rows
#I have a dataframe which has one row, and several columns. Some of the columns are single values, and others are lists. All list columns are the same length. I want to split each list column into a separate row, while keeping any non-list column as is.

#** refernce :- https://stackoverflow.com/questions/59607979/convert-an-array-column-to-array-of-structs-in-pyspark-dataframe
# ** https://www.semicolonworld.com/question/53774/pyspark-split-multiple-array-columns-into-rows

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
# from pyspark.sql.functions import explode, col, struct
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()


df = spark.createDataFrame([Row(a=1, b=[1,2,3],c=[7,8,9], d='foo')])
# +---+---------+---------+---+
# |  a|        b|        c|  d|
# +---+---------+---------+---+
# |  1|[1, 2, 3]|[7, 8, 9]|foo|
# +---+---------+---------+---+

# what we want
# +---+---+----+------+
# |  a|  b|  c |    d |
# +---+---+----+------+
# |  1|  1|  7 |  foo |
# |  1|  2|  8 |  foo |
# |  1|  3|  9 |  foo |
# +---+---+----+------+

# if we go with this , # So we can not do this
df_exploded = df.withColumn('b', explode('b'))\
    .withColumn("c",explode(col("c"))
)# will create curr_row_count X no_of_items_in_array
df_exploded.show(truncate=False)

# way 1 ,
from pyspark.sql.functions import arrays_zip, array

df = df.withColumn("merged_col" , arrays_zip("b","c"))
df.show(truncate=False)
df.withColumn("added_array" , concat(col("b") , col("c")) ).show()
flattened_df = df.withColumn("exploded_merged_col" , explode(col("merged_col")))
flattened_df.show(truncate=False)
flattened_df.printSchema()
flattened_df.select(
    col("a"),col("exploded_merged_col.b"),col("exploded_merged_col.c"),col("d")
).show()

# way 2 , this is useful to manipulate any col in DF , useful for any arbritary number of col and items in it . esle use array_zip(c1,c2) as seen before

df = spark.createDataFrame([Row(a=1, b=[1,2,3],c=[7,8,9], d='foo')])

def zip_and_explode(*colnames, n):
    return explode(array(*[
        struct(*[col(c).getItem(i).alias(c) for c in colnames])
        for i in range(n)
    ]))


df = df.withColumn("tmp", zip_and_explode("b", "c", n=3))
df.show(truncate=False)
df.select(
    col("a"),col("tmp.b"),col("tmp.c"),col("d")
).show()






