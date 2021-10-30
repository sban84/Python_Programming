##  VERY IMPORTANT:: Check all the code , remember the way its done
# Very good example
# ###########################
## NOTE for sum and max after groupby() we dont need agg

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, count, countDistinct, avg
from pyspark.sql.types import *

filamentData = [['filamentA', '100W', 605],
                ['filamentB', '100W', 683],
                ['filamentB', '100W', 691],
                ['filamentB', '200W', 561],
                ['filamentA', '200W', 530],
                ['filamentA', '100W', 619],
                ['filamentB', '100W', 686],
                ['filamentB', '200W', 600],
                ['filamentB', '100W', 696],
                ['filamentA', '200W', 579],
                ['filamentA', '200W', 520],
                ['filamentA', '100W', 622],
                ['filamentA', '100W', 668],
                ['filamentB', '200W', 569],
                ['filamentB', '200W', 555],
                ['filamentC', '50W', 100]]

print(type(filamentData))
spark = SparkSession.builder.appName("Test").master("local").getOrCreate()
df = spark.createDataFrame(filamentData).toDF("FilamentType", "BulbPower", "LifeInHours")
df.show(truncate=False)

# filamentRDDofROWs = filamentDataRDD.map(lambda x :Row(str(x[0]), str(x[1]), str(x[2])))

df = df.withColumn("LifeInHours", col("LifeInHours").cast(FloatType()))
df.show(truncate=False)

print(df.columns)

# filter test
onehundred_watt_bulb = df.filter(df["BulbPower"] == "100W")
onehundred_watt_bulb.show(truncate=False)

onehundred_watt_bulb_greaterlife = df.filter((df["BulbPower"] == "100W")
                                             & (df["LifeInHours"] > 650))
onehundred_watt_bulb_greaterlife.show(truncate=False)
df.printSchema()
# Challenge 1 Calculate summary statistics on a numerical column
print("Calculate summary statistics on a numerical column-->")
df.describe().show(truncate=False)
# • Count the frequency of distinct values in the FilamentType categorical column
print("Count the frequency of distinct values in the FilamentType categorical column")
distinct_count = df.groupby("FilamentType").count()
distinct_count.show()

# countDistinct will give the count of unique values for that col.
distinct_count_1 = df.groupby("FilamentType").agg(countDistinct("BulbPower").alias("cnt_by_type"))
distinct_count_1.show()

# • Count the frequency of distinct values in the BulbPower categorical column
## Note : remember the way to loop through DF , VERY IMPORTANT

distinct_count_2 = df.groupby("BulbPower").count()
distinct_count_2.show()

for row in distinct_count_2.collect(): # Row(BulbPower='200W', count=8)
    print(f"way to get the count from DF, BulbPower type = {row['BulbPower']}"
          f"and its count = {row['count']}")

#################### VERY IMPORTANT ###########################
## Some random challenge , for agg function

df.groupby(df["FilamentType"]).agg(avg("LifeInHours")).show(truncate=False)
# withColumn will replace if there is already col name with same name
df.withColumn("LifeInHours" , col("LifeInHours").cast(DoubleType()))
# NOTE for sum and min/max after groupby() we dont need agg as below
df.groupby("FilamentType").sum("LifeInHours").show(truncate=False)
df.groupby("FilamentType").agg(countDistinct("LifeInHours")).show(truncate=False)
# NOTE for sum and max after groupby() we dont need agg as below
df.groupby("FilamentType").max("LifeInHours").show(truncate=False)

df.groupby("FilamentType").agg(count("LifeInHours").alias("cnt")) \
    .sort(col("cnt"), ascending=False).show(truncate=False)

df.groupby(col("FilamentType")).agg(round(avg(col("LifeInHours"))).alias("avg_rounded")).show(truncate=False)

# Try creating table in-memory in Spark side and make use of the SQL syntax to do the same.
df.createOrReplaceTempView("bulb_table")
sql_1 = spark.sql("select FilamentType, count(distinct LifeInHours) as cnt  from bulb_table group by FilamentType")
sql_1.show(truncate=False)

# Computing Average, with round function.
sql_2 = spark.sql(" select FilamentType , round(avg(LifeInHours)) as avg_life  from bulb_table "
                  "group by FilamentType limit 5")
sql_2.show(truncate=False)
