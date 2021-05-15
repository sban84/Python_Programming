from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

# NOTE most of the api in pyspark accepts col("col_name_str)
# better to follow one style like this
spark = SparkSession.builder.appName("Test5").master("local").getOrCreate()

df_miss = spark.createDataFrame([
(1, 143.5, 5.6, 28, 'M',100000),
(2, 167.2, 5.4, 45,'M', None),
(3, None , 5.2, None, None, None),
(4, 144.5, 5.9, 33, 'M', None),
(5, 133.2, 5.7, 54, 'F', None),
(6, 124.1, 5.2, None, 'F', None),
(7, 129.2, 5.3, 42,'M', 76000),
(7, 129.2, 5.3, 42,'M', 76000),
(8, 129.2, 5.3, 42,'M', 76000)
] , ['id', 'weight', 'height', 'age', 'gender', 'income'])

df_miss.show(truncate=False)

# Challenge 1 # get the row count and distinct row count
item_count = df_miss.count()
item_distinct_count = df_miss.distinct().count()
print(f"count {item_count} and dis count {item_distinct_count}")

df_miss.agg(count('id').alias('count'), countDistinct('id').alias('distinct')).show()

# Challenge 2 # now find out the records which are fully duplicated
comom_col = [col(c) for c in df_miss.columns ]
all_col_grp_count = df_miss.groupby(df_miss.columns).agg(count("*").alias("count"))
# since the col names are same in both df so passing a list of names will be easy .
# and also it will avoid duplicate col names come twice in the o/p df.

# join_with_original_df: DataFrame = df_miss.join(all_col_grp_count ,
#                                                 (df_miss["id"] == all_col_grp_count["id"] ) ,
#                                                 "inner")

# join_with_original_df: DataFrame = df_miss.join(all_col_grp_count ,
#                                                 df_miss.columns ) ,
#                                                 "inner")

join_with_original_df: DataFrame = df_miss.groupby(df_miss.columns).agg(
    count("*").alias("count")
)


join_with_original_df.filter(col("count") > 1).show(truncate=False)

# +---+------+------+---+------+------+-----+
# |id |weight|height|age|gender|income|count|
# +---+------+------+---+------+------+-----+
# |7  |129.2 |5.3   |42 |M     |76000 |2    |


## But if we drop this where count > 1 then we will loose data original + duplicated one.
# so to drop duplicates in DF , we must use df.dropDuplicates() OR  df.dropDuplicates("colName") for any col specific

df_miss = df_miss.dropDuplicates().drop("count")
# check after dropDuplicates() is there any rec with all columns count >1
df_miss.groupby(df_miss.columns).agg(count("*").alias("count")).filter(col("count") > 1).show()

# +---+------+------+---+------+------+-----+
# | id|weight|height|age|gender|income|count|
# +---+------+------+---+------+------+-----+
# +---+------+------+---+------+------+-----+

df_miss.orderBy(col("id") , ascending= True).show()



