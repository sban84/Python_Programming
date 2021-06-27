"""
find each col have how many null values , we can enhance the condition ,
but main logic how we are calculating with when clause. we are assigning 1 for null value
and finally doing count those 1's
It will be very generic logic where we need to do any analysis based on some condition,
use when / otherweise logic and apply count / sum or any other logic that may be useful
as per the requirement.

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import  *

spark = SparkSession.builder.appName("Test5").master("local").getOrCreate()
# find each col have how many null values , we can enhance the condition ,
# but main logic how we are calculating with when clause. we are assigning 1 for null value
# and finally doing count those 1's
#
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

df_miss.agg(sum("age").alias("total_age_complete_df")).show()

result = [ count(when( col(c).isNull() , 1)).alias(c) for c in df_miss.columns]
print(result)
df_miss.select(result).show()

# df_miss.filter(col("age").isNull()).show()

# doing the same as above but in more code breakup , more verbose

df_miss.withColumn("no_age_null" , when(col("age").isNull() , 1)).agg(count(col("no_age_null"))).show()
