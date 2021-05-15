from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data = [
    (1,"2015-05-03" , "2021-05-05"),
    (2,"2006-02-01" , "2011-02-01"),
    (3,"2016-02-01" , "2017-02-01"),
    (4,"2021-04-06" , "2021-05-02"),
]

df = spark.createDataFrame(data).toDF("id" , "start_date" , "end_date")
df.show(truncate=False)

# 1, calculate the date diff.
with_days_diff_df = df.withColumn("days_diff" , datediff(col("end_date") , col("start_date")))
with_days_diff_df.show()

df.withColumn("months" , month(col("end_date"))).show()
df.withColumn("months_diff" , months_between(col("end_date") , col("start_date"), roundOff=False)).show()

#2. filter rec whose moths diff > 9 months

df.filter(months_between(col("end_date") , col("start_date")) > 9).show()

from dateutil.relativedelta import relativedelta
from datetime import datetime
def months_diff_by_python_code(start, end) :
    d1 = datetime.strptime(start, "%Y-%m-%d")
    d2 = datetime.strptime(end, "%Y-%m-%d")
    r = relativedelta(d2,d1)
    print(r.months)
    mon_diff = r.months + (12 * r.years)
    if r.months > 0:
        mon_diff += 1
    return mon_diff

months_diff_by_python_code("2016-02-01" , "2017-02-01")

df.withColumn("months_diff" , round(months_between(col("start_date") , col("end_date")))).show()



