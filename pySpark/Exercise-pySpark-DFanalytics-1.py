from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import WindowSpec, Window
# Very good example
spark = SparkSession.builder.master("local").appName("Test6").getOrCreate()

df = spark.createDataFrame([
    (1, 143.5, 5.6, 28, 'M', 100000),
    (2, 167.2, 5.4, 45, 'M', None),
    (3, None, 5.2, None, None, None),
    (4, 144.5, 5.9, 33, 'M', None),
    (5, 133.2, 5.7, 54, 'F', None),
    (6, 124.1, 5.2, None, 'F', None),
    (7, 129.2, 5.3, 42, 'M', 76000),
    (7, 129.2, 5.3, 42, 'M', 76000),
    (8, 129.2, 5.3, 42, 'M', 46000),
    (None, 129.2, 5.3, 42, 'M', 76000)
], ['id', 'weight', 'height', 'age', 'gender', 'income'])

df.show(truncate=False)

# Challenge 1 #  cleaning , removing dups and also none for key col

# as we can see many col has null but depending on use case we need to drop the rec which has None
# like id can not be none so drop by col if any col has none

df_valid = df.dropna(how="any", subset=["id"])
df_valid.show()

# NOTE : Remember check full duplicated rec , by simple count , good way to get the which rec is duplicated
(df_valid.groupby(df_valid.columns).agg(count("*").alias("count"), countDistinct("*").alias("distinct_count"))
 ).show()

no_duplicate = df_valid.dropDuplicates()  # df_valid.dropDuplicates(subset=["id"]) for any col

(no_duplicate.groupby(df_valid.columns).agg(count("*").alias("count"), countDistinct("*").alias("distinct_count"))
 ).show()

# Challenge 1 #  avg by Male/ Female

avg_weight_by_gender = no_duplicate.groupby(col("gender")).agg(round(avg(col("weight")), 2).alias("avg_weight"))
avg_weight_by_gender.dropna().orderBy(col("gender"), ascending=True).show(truncate=False)

# NOTE - filter weight > 130 , some important analytics VERY USEFUL REMEMBER
high_weight = no_duplicate.filter(col("weight") > 130).orderBy(col("weight"), ascending=False)
print(high_weight.collect()[0])


# get age of that rec highest weight

class Person:
    def __init__(self, id, weight, height, age, gender, income):
        self.id = id
        self.weight = weight
        self.height = height
        self.age = age
        self.gender = gender
        self.income = income


def iterate_df_rows(dataframe: DataFrame):
    for row in dataframe.collect():
        print(row)
        # test just to create class and like POJO
        Person(row["id"], row["weight"], row["height"], row["age"], row["gender"], row["income"])


def get_top_row(df: DataFrame, orderBycol: str, c: str):
    # if the df is not sorted then sort first by desc
    df = df.orderBy(col(orderBycol), ascending=False)
    return df.collect()[0][c]


# way 1
age_of_highest_weight = high_weight.collect()[0]["age"]
print(age_of_highest_weight)
iterate_df_rows(high_weight)
# way 2, same but just by func call
age_of_highest_weight = get_top_row(high_weight, "weight", "age")
print(age_of_highest_weight)

# Challenge 2 , NOTE - new col with sal_category using when / otherwise like case when in SQL

with_sal_catogory = no_duplicate.withColumn("sal_category",
                                            when(col("income").isNull(), None)
                                            .when((col("income") > 80000) & (col("income").isNotNull()), "Good")
                                            .when(( col("income")  > 50000) & (col("income") < 80000) , "Average")
                                            .when( ( col("income")  < 50000) & ( col("income").isNotNull()) , "Below")
                                            .otherwise("NA"))

with_sal_catogory.show(truncate=False)
with_sal_catogory = with_sal_catogory.filter(col("sal_category").isNotNull())
with_sal_catogory.show(truncate=False)

## Challenge 3,  Rank()

spec = Window.partitionBy(col("weight")).orderBy(col("income"))
with_rank = no_duplicate.withColumn("rn" , rank().over(spec))
with_rank.show(truncate=False)