from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, struct
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.appName("Test5").master("local").getOrCreate()

def sum(x, y):
    return x + y

sum_cols = udf(sum, IntegerType())

a=spark.createDataFrame([(101, 1, 16)], ['ID', 'A', 'B'])
a.show()
a.withColumn('Result', sum_cols('A', 'B')).show()