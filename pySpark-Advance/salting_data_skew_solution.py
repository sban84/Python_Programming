from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.storagelevel


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
                                                         lit(floor(rand(seed) * 3))))
df_salted.cache()

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
