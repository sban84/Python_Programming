# Very good exmaple
# rdd , how to convert rdd to DF , or sample manual data to DF
# apply udf on single col and multiple cols logic , Remember THIS

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Temp_avg_for2_col").master("local").getOrCreate()

tempData = [59, 57.2, 53.6, 55.4, 51.8, 53.6, 55.4]
rdd = spark.sparkContext.parallelize(tempData, 4)
print(rdd.collect())
print(rdd.getNumPartitions())


# f = c * 1.8 + 32
def to_ferenhite(c):
    return (c * 1.8) + 32


f_temp_rdd = rdd.map(lambda x: to_ferenhite(x))
print(f_temp_rdd.collect())

# 2. Same problem do it using DF

df = spark.createDataFrame(rdd, StringType()).toDF(
    "id")  # id the underlying data have multiple types like hee int anf float then use StringTYpe else will not work
# udf_func = udf(to_ferenhite,DoubleType())
udf_func = udf(lambda x: to_ferenhite(x),
               DoubleType())  # remember this , same as above way of doing the same , but this is more readble way .
df = df.withColumn("temp_fern", udf_func(col("id").cast(DoubleType())))
df.show(truncate=False)

studentMarksData = [["si1", "year1", 62.08, 62.4], ["si1", "year2", 75.94, 76.75],
                    ["si2", "year1", 68.26, 72.95],
                    ["si2", "year2", 85.49, 75.8],
                    ["si3", "year1", 75.08, 79.84], ["si3", "year2", 54.98, 87.72], ["si4", "year1", 50.03, 66.85],
                    ["si4", "year2", 71.26, 69.77], ["si5", "year1", 52.74, 76.27], ["si5", "year2", 50.39, 68.58],
                    ["si6", "year1", 74.86, 60.8], ["si6", "year2", 58.29, 62.38], ["si7", "year1", 63.95, 74.51],
                    ["si7", "year2", 66.69, 56.92]]

stu_rdd = spark.sparkContext.parallelize(studentMarksData, 2)


# Average grades per semester, each year, for each student
def multiple_col_avg(x1, x2):
    return (x1 + x2) / 2


# using rdd
multiple_col_avg_udf = udf(multiple_col_avg, DoubleType())
avg_grades = stu_rdd.map(lambda x: [x[0], x[1], (x[2] + x[3]) / 2])
print(avg_grades.collect())

# using df
df = spark.createDataFrame(studentMarksData, ["id", "year", "score1", "score2"])
df.show()
avg_score_by_id_year = df.withColumn("avg_marks", multiple_col_avg_udf(col("score1").cast(IntegerType()),
                                                                       col("score2").cast(IntegerType())),
                                     )
avg_score_by_id_year.show(truncate=False)

# • Top three students who have the highest average grades in the second year

filter_sec_year_students = avg_score_by_id_year.filter(col("year") == "year2").orderBy(col("avg_marks"), ascending=False)
filter_sec_year_students.show(truncate=False)

# • Bottom three students who have the lowest average grades in the second year
filter_sec_year_students = avg_score_by_id_year.filter(col("year") == "year2").orderBy(col("avg_marks"), ascending=True)
filter_sec_year_students.show(truncate=False)

# • All students who have earned more than an 80% average in the second semester of the second year
avg_score_by_id_year.filter( (col("year") == "year2") & (col("score2") > 80) ).show()
