import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

# spec which will have order by , for some func like rank(), row_number() and dense_rank()
# orderBy is must, because with that it decides the order.
# partitionBy is optional , if not passed then window will be applied
# on the whole DF , else DF will be divided by partitionBy cols.
# Very good example
spark = SparkSession.builder.config("spark.driver.bindAddress", "localhost").getOrCreate()
emp_data = [("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 4100),
            ("Maria", "Finance", 3000),
            ("James1", "Sales", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000),
            ("Saif", "Sales", 4100)
            ]

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=emp_data, schema=columns)
df.show(truncate=False)

spec = Window.partitionBy(col("department")).orderBy(col("salary").desc())

"""row_number()"""
df_row_number = df.withColumn("row_number", row_number().over(spec))
df_row_number.show()

"""rank() example"""
df_with_rank = df.withColumn("rank", rank().over(spec))
df_with_rank.show(truncate=False)

"""dense_rank()"""
df_dense = df.withColumn("dense_rank", dense_rank().over(spec))
df_dense.show(truncate=False)

# without partitionBy , it will be across DF . NO window / partition bases
print(" ******* without partitionBy ******** ")
spec_1 = Window.orderBy("salary")

"""row_number()"""
df_row_number = df.withColumn("row_number", row_number().over(spec_1))
df_row_number.show()

"""rank() example"""
df_with_rank = df.withColumn("rank", rank().over(spec_1))
df_with_rank.show(truncate=False)

"""dense_rank()"""
df_dense = df.withColumn("dense_rank", dense_rank().over(spec_1))
df_dense.show(truncate=False)

# Lead and lag function , like e.g know previous purchase by an customer on multiple visits.
# e.g. partitionBy(col("customer")).orderBy(col("txn_date"))
# withColumn("prev_pur" , lag(col("amount"),1).over(spec)))

df_lag = df.withColumn("previous_sal", lag(col("salary"), 1, "default").over(spec))
df_lag.show()

## cumulative sum

data = [
    ("fruit", "apple", "100"),
    ("fruit", "banana", "50"),
    ("fruit", "orange", "30"),
    ("veg", "potato", "20"),
    ("veg", "onion", "30"),
    ("dairy", "milk", "20"),
    ("dairy", "butter", "50")
]

df = spark.createDataFrame(data, ["type", "name", "price"])
df.show()

spec_2 = Window.partitionBy(col("type")).orderBy(col("name")).rowsBetween(-sys.maxsize, 0)
# spec_2 = Window.rowsBetween(-sys.maxsize,0) # DF level no partitionBy
df = df.withColumn("cum_sum", sum(col("price")).over(spec_2))
df.show(truncate=False)
