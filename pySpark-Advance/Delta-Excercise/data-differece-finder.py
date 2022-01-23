from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark():
    spark = SparkSession.builder.appName("data-difference-finder"). \
        master("local").getOrCreate()
    return spark


# data_1 = [(1, "a", 100, "2021-12-01"),
#           (2, "b", 200, "2021-12-02"),
#           (3, "c", 300, "2021-12-03")]

data_2 = [(1, "a", 200, "2021-12-01"),
          (11, "d", 210, "2021-12-02"),
          (3, "c", 300, "2021-12-01")
          ]

spark = create_spark()

# schema = StructType([
#     StructField("id", IntegerType(), False),
#     StructField("name", StringType(), False),
#     StructField("amount", IntegerType(), False),
#     StructField("txn_date", StringType(), False),
# ])
# df1 = spark.read.schema(schema).option("header", "True").csv("./target_dir/2021-12-01/sales.txt")
# df2 = spark.createDataFrame(data_2, schema)

# NOTE: keep in mind that the values does not have space, as while reading files into spark.
df1 = spark.read.option("header", "True").csv("./target_dir/2021-12-01/sales.txt")
df2 = spark.createDataFrame(data_2, ["id", "name", "amount", "txn_date"])

df1.show()
df2.show()

diff = df2.exceptAll(df1)
# diff = diff.withColumn("day", current_date())
diff.show()
# diff.write.mode("append").partitionBy("day").save("./target_dir/")

df1 = df1.withColumnRenamed("id", "df1_id")
joined_df = df2.join(df1, df2.id == df1.df1_id, "left_outer")
#joined_df = df2.join(df1, df2["id"] == df1["df1_id"], "left_outer")
joined_df.show()

# | id|name|amount|  txn_date|  id|name|amount|  txn_date|
# +---+----+------+----------+----+----+------+----------+
# |  1|   a|   200|2021-12-01|   1|   a|   100|2021-12-01|
# | 11|   d|   210|2021-12-02|null|null|  null|      null|
# |  3|   c|   300|2021-12-01|   3|   c|   300|2021-12-01|
# +---+----+------+----------+----+----+------+----------+

# From the above o/p it's easy ti find which rec are new and which rec are updated
# to find new filter df_1.id_== null

new_rec = joined_df.filter(col("df1_id").isNull())
new_rec.show()
