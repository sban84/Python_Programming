"""
Very good code ,
1. how to create a col with array of items to flatten into multiple cols
by first converting to array[struct_col] then explode that new col.
so that we can use new_col.item for better use.
2. Find out the duplicated rec by 2 ways , self join & other one group and then filter
"""

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, array, array_contains, struct, flatten, rank, row_number, count
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data = [
    (1, [20, 30, 20], "sban"),
    (2, [50, 60], "rohit"),
    (3, [50, 40, 60], "shyam")
]

df = spark.createDataFrame(data, ["id", "scores", "name"])
df.printSchema()
df.select(col("scores").getItem(0)).show()

# 1. array(*cols)
df.withColumn("id_name_combined", array(col("id"), col("name"))).show()
# 2. array_contains(col, value)
df.filter(array_contains(col("scores"), 50)).show()


# 3. find the student ids whose 1st year score is same

# for expanding a array of items to individual cols, we need to first convert array[struct],
# then call expand(array_col) , then we can select by temp.first_year way.
def expand_array_col_into_seperate_col(colName):
    result = array([struct(col(colName).getItem(0).alias("first_year"),
                           col(colName).getItem(1).alias("sec_year"))
                    ])
    return result


df = spark.createDataFrame(data, ["id", "scores", "name"])
df.withColumn("temp", expand_array_col_into_seperate_col("scores")).show(truncate=False)
df.withColumn("temp", expand_array_col_into_seperate_col("scores")).printSchema()

flattended_df = df.withColumn("temp", explode(expand_array_col_into_seperate_col("scores")))
flattended_df.printSchema()
flattended_df.show(truncate=False)
flattended_df = flattended_df.select(col("id"), col("name"), col("temp.first_year"), col("temp.sec_year"))
flattended_df.show(truncate=False)

# join_df = flattended_df.join(
#     flattended_df.groupby(col("first_year").agg((count("*") > 1).cast(IntegerType()).alias("is_duplicated")) )
#     , ["id","name"],"inner"
# )
join_df = flattended_df.join(
    flattended_df.groupBy("first_year").agg((count("*") > 1).cast("int").alias("Duplicate_indicator")),
    on=["first_year"],
    how="inner"
)
print("after self join ******")
join_df.show()
join_df.filter(col("Duplicate_indicator") == 1).show()

# another way without self jon.... anything is okay , but
# join logic is useful in some cases so remember
print("another way without join")
flattended_df = flattended_df.withColumnRenamed("first_year", "first_year_score")
flattended_df.printSchema()
rec_with_count = flattended_df.groupby(col("first_year_score")).agg(count("*").alias("count")).filter(col("count") > 1)
rec_with_count.show()
print(rec_with_count.rdd.collect())
print(rec_with_count.rdd.collect()[0])

for row in rec_with_count.rdd.collect():
    res = flattended_df.filter(col("first_year_score") == row[0])

res.show()

# 4.array_contains(col, value)

df.withColumn("contains_test", array_contains(col("scores"), 50)).show()

# for struct type ,
# data = [
#     (1, (20,30)),
#     (2, (50,60))
# ]
# df =  spark.createDataFrame(data , ["a" , "b"])
# df.printSchema()
