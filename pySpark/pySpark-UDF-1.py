from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf ,struct , col
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder.appName("Test5").master("local").getOrCreate()

df1 = spark.createDataFrame(
    [(1, "a", 2.0), (2, "b", 3.0), (3, "c", 3.0)],
    ("x1", "x2", "x3"))

df2 = spark.createDataFrame(
    [(1, "f", -1.0), (2, "b", 0.0)], ("x1", "x2", "x3"))

df = df1.join(df2, (df1.x1 == df2.x1) & (df1.x2 == df2.x2), "inner")
df.show()


data =  [
    (1,"a" , "10" , "20"),
    (1,"a" , "20" , "20"),
    (2,"b" , "100" , "200"),
    (2,"c" , "100" , "300")
]

df = spark.createDataFrame(data , ["id" , "name" , "s1" , "s2"])
df.show(truncate=False)

# challenge 1 , Very importamt passing 2 col to udf
sum_cols = udf(lambda x: x[0]+x[1], IntegerType())
a=spark.createDataFrame([(101, 1, 16) , (201,10,12)], ['ID', 'A', 'B'])
a.show()
a.withColumn('Result', sum_cols(struct('A', 'B'))).show()

# challenge 2 # Very importamt passing one col to udf
def f(x):
    return x +1

add_one = udf(lambda x : f(x) , IntegerType())

df = df.withColumn("added_col" , add_one(col("s1").cast(IntegerType())))
df.show()

# another udf
avg_marks_udf = udf(lambda x : (x[0]+x[1])/2 , DoubleType())
df=df.withColumn("avg_marks" , avg_marks_udf( struct(col("s1").cast(IntegerType()),col("s2").cast(IntegerType())) ))
df.show()

