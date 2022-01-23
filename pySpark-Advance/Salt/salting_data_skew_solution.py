from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.storagelevel
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("salting").master("local").getOrCreate()

skew_data = [
    ("a", 100, "2021-01-01"),
    ("a1", 10, "2021-01-01"),
    ("a2", 1, "2021-01-01"),
    ("a3", 200, "2021-01-01"),
    ("a4", 1000, "2021-01-01"),
    ("b", 22, "2021-01-02"),
    ("c", 30, "2021-01-03"),
]

schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("purchase_date", StringType(), True)
    ]
)
df = spark.createDataFrame(skew_data, schema=schema)
df.show()
df.printSchema()
print(f"df.rdd.getNumPartitions() {df.rdd.getNumPartitions()}")
df = df.withColumn("purchase_date", to_date(col("purchase_date")))
df.printSchema()
df.show()

data = [
    ("2021-01-01", "in_store"),
    ("2021-01-02", "in_store"),
    ("2021-01-03", "in_store")
]
df_2 = spark.createDataFrame(data, ["date", "source"])
df_2.show()

print("\n Joining using salting technique")
seed = 123456
df_salted = df.withColumn("purchase_date_salted", concat(col("purchase_date"), lit("_"),
                                                         lit(floor(rand(
                                                         ) * 3))))  # seed) is a number (or vector) used to initialize a pseudorandom number generator. For a seed to be used in a pseudorandom number generator, it does not need to be random
df_salted.cache()
df_salted.show(truncate=False)


def getModifiedRightData(right: DataFrame):
    columnList = []
    for i in range(3):
        columnList.append(lit(i))
    return right.withColumn("salted", explode(array(columnList)))


df_2_exploded = getModifiedRightData(df_2)
df_2_exploded.show()

# +----------+--------+------+
# |      date|  source|salted|
# +----------+--------+------+
# |2021-01-01|in_store|     0|
# |2021-01-01|in_store|     1|
# |2021-01-01|in_store|     2|
# |2021-01-02|in_store|     0|
# |2021-01-02|in_store|     1|
# |2021-01-02|in_store|     2|
# |2021-01-03|in_store|     0|
# |2021-01-03|in_store|     1|
# |2021-01-03|in_store|     2|
# +----------+--------+------+

join_result = df_salted.join(df_2_exploded, df_salted.purchase_date_salted == (
    concat(df_2_exploded.date, lit("_"), df_2_exploded.salted)), "inner")

join_result.orderBy(to_date(col("purchase_date")).asc()).show(truncate=False)
join_result.orderBy(to_date(col("purchase_date")).asc()).explain()

# +----+------+-------------+--------------------+----------+--------+------+
# |name|amount|purchase_date|purchase_date_salted|date      |source  |salted|
# +----+------+-------------+--------------------+----------+--------+------+
# |a   |100   |2021-01-01   |2021-01-01_2        |2021-01-01|in_store|2     |
# |a1  |10    |2021-01-01   |2021-01-01_1        |2021-01-01|in_store|1     |
# |a3  |200   |2021-01-01   |2021-01-01_1        |2021-01-01|in_store|1     |
# |a2  |1     |2021-01-01   |2021-01-01_0        |2021-01-01|in_store|0     |
# |a4  |1000  |2021-01-01   |2021-01-01_0        |2021-01-01|in_store|0     |
# |c   |30    |2021-01-03   |2021-01-03_0        |2021-01-03|in_store|0     |
# |b   |22    |2021-01-02   |2021-01-02_0        |2021-01-02|in_store|0     |
# +----+------+-------------+--------------------+----------+--------+------+


# Another way , Ex-2, Creating unique sequential number to each row in the DF
df_salt_mechanism_1 = df.withColumn("mono_id", monotonically_increasing_id())
df_salt_mechanism_1.show(truncate=False)

winSpec = Window.orderBy(col("purchase_date").asc())
df_salt_mechanism_2 = df.withColumn("row_number", row_number().over(winSpec))
df_salt_mechanism_2 = df_salt_mechanism_2. \
    withColumn("purchase_date_salt", concat(df_salt_mechanism_2.purchase_date, lit("_"),
                                            df_salt_mechanism_2.row_number % 3)
               )
df_salt_mechanism_2.show(truncate=False)
# +----+------+-------------+----------+------------------+
# |name|amount|purchase_date|row_number|purchase_date_salt|
# +----+------+-------------+----------+------------------+
# |a   |100   |2021-01-01   |1         |2021-01-01_1      |
# |a1  |10    |2021-01-01   |2         |2021-01-01_2      |
# |a2  |1     |2021-01-01   |3         |2021-01-01_0      |
# |a3  |200   |2021-01-01   |4         |2021-01-01_1      |
# |a4  |1000  |2021-01-01   |5         |2021-01-01_2      |
# |b   |22    |2021-01-02   |6         |2021-01-02_0      |
# |c   |30    |2021-01-03   |7         |2021-01-03_1      |
# +----+------+-------------+----------+------------------+

