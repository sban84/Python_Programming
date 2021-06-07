"""
Very good code
This has some good logic to know about pySpark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("pySpark Recipes").master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# inp = input("Enter A Word")
inp = "Hello"
print(inp)
# data = inp.split()
data = list(inp)
print(data)


# 1 . create a paired RDD, in which the keys are elements of a single RDD, and the value of a key is 0 if the element
# is a consonant, or1 if the element is a vowel

def vowel_checker(c):
    vowel_list = ['a', 'e', 'i', 'o', 'u']
    if c in vowel_list:
        return 1
    else:
        return 0


rdd = spark.sparkContext.parallelize(data, 2)
result = rdd.map(lambda x: (x, vowel_checker(x)))
print(result.collect())

df = spark.createDataFrame(data, StringType()).toDF("c")
vowel_checker_udf = udf(lambda x: vowel_checker(x), IntegerType())
df.withColumn("isVowel", vowel_checker_udf(col("c").cast(StringType()))).show()

# TODO NOT working need to check another way without using udf functions # https://stackoverflow.com/questions/48282321/valueerror-cannot-convert-column-into-bool
# df.withColumn("isVowel" , when(lit(vowel_checker("c")) == 1 , 1).otherwise(0) ).show()

filDataSingle = [
    ['filamentA', '100W', 605], ['filamentB', '100W', 683],
    ['filamentB', '100W', 691], ['filamentB', '200W', 561], ['filamentA', '200W', 530],
    ['filamentA', '100W', 619], ['filamentB', '100W', 686], ['filamentB', '200W', 600],
    ['filamentB', '100W', 696], ['filamentA', '200W', 579], ['filamentA', '200W', 520],
    ['filamentA', '100W', 622], ['filamentA', '100W', 668], ['filamentB', '200W', 569],
    ['filamentB', '200W', 555], ['filamentA', '200W', 541]
]
df = spark.createDataFrame(filDataSingle, ["FilamentType", "BulbPower", "LifeInHours"])
df.show()
# 2.  Finding the Mean/avg Lifetime Based on Bulb Power
df.withColumn("LifeInHours", col("LifeInHours").cast(DoubleType()))
df.groupby(col("BulbPower")).agg(avg(col("LifeInHours")).alias("avg_life")).orderBy(col("avg_life"), ascending=False) \
    .show(truncate=False)

# 3. JOIN

students_df = spark.read.option("infereSchema", True).option("header", True).csv("./students.csv")
students_df.dropna().show()
subjectsData = [
    ['si1', 'Python'], ['si3', 'Java'], ['si1', 'Java'], ['si2', 'Python'], ['si3', 'Ruby'],
    ['si4', 'C++'], ['si5', 'C'], ['si4', 'Python'], ['si2', 'Java'], ['s10', 'Scala']
]
subjectsData_df = spark.createDataFrame(subjectsData, ["Student_Id", "Subject_Name"])
subjectsData_df.show()

inner = students_df.join(subjectsData_df, students_df.Student_Id == subjectsData_df.Student_Id) \
    .drop(subjectsData_df.Student_Id)
inner.show()

# 4. is there any student who does not have any subjects assigned
left_outer_exmaple = students_df.join(subjectsData_df,
                                      students_df.Student_Id == subjectsData_df.Student_Id, "left")

left_outer_exmaple.filter((col("Subject_Name").isNull()) | (col("Subject_Name") == "")).show()

# Right join same as left but better to use left join based on which table you wanted to refer as in left side
# that's it.

# 5. just to see is there any record present in Subject data but not in students data
students_df =  students_df.withColumnRenamed("Student_Id" , "Id")
subjectsData_df.join(students_df , subjectsData_df.Student_Id == students_df.Id,
                     "left").filter(col("Name").isNull()).show()

