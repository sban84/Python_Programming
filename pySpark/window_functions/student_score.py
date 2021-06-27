# Reference , NOTE
# >> one practical use case when to use is , lets say we need to get the top 3 students
# irrespective of the equal score students.
# But, if we need to get Top students considering the fact that equal scores will be treated as
# same and we don't want to increment the counter for same value records.

# The difference (row_number, rank and dense_rank ) is when there are "ties" in the ordering column. Check
# student_score.py difference between rank and dense rank , rank includes gap when same value found but dense_rank
# does not include gap . that's it.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

studentMarksData = [["si1", "year1", 62.08, 62.4], ["si1", "year2", 75.94, 76.75],
                    ["si2", "year1", 68.26, 72.95],
                    ["si2", "year2", 85.49, 75.8],
                    ["si3", "year1", 75.08, 79.84], ["si3", "year2", 71.26, 69.77], ["si4", "year1", 50.03, 66.85],
                    ["si4", "year2", 71.26, 69.77], ["si5", "year1", 52.74, 76.27], ["si5", "year2", 50.39, 68.58],
                    ["si6", "year1", 74.86, 60.8], ["si6", "year2", 58.29, 62.38], ["si7", "year1", 63.95, 74.51],
                    ["si7", "year2", 66.69, 56.92]]

# Average grades per semester, each year, for each student
def multiple_col_avg(x1, x2):
    return (x1 + x2) / 2

multiple_col_avg_udf = udf(multiple_col_avg, DoubleType())

# using df
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(studentMarksData, ["id", "year", "score1", "score2"])
df.show()
avg_score_by_id_year = df.withColumn("avg_marks", multiple_col_avg_udf(col("score1").cast(IntegerType()),
                                                                       col("score2").cast(IntegerType())),)
avg_score_by_id_year.show(truncate=False)


# Calculate Top student using  rank function ( use case where we just need to calculte on the whole DF )
print("Calculate Top student using  rank function")
windowSpec = Window.orderBy(col("avg_marks"))
students_with_rank = avg_score_by_id_year.withColumn("rank" , rank().over(windowSpec))
students_with_rank.show()

sec_year_students_with_rank = avg_score_by_id_year.filter(col("year") == "year2").withColumn("rank" , rank().over(windowSpec))
sec_year_students_with_rank.filter(col("rank") <= 3).show()

sec_year_students_with_row_number = avg_score_by_id_year.filter(col("year") == "year2")\
    .withColumn("row_number", row_number().over(windowSpec))
sec_year_students_with_row_number.show()

# Calculate Top student using  rank function ( use case where we need to divide/partition the data on some col for example by year,
# or any other condition)
print("Window By Partition ")
windowSpec = Window.partitionBy(col("year")).orderBy(col("avg_marks").desc())
students_by_yearly_rank = avg_score_by_id_year.withColumn("rank", rank().over(windowSpec))
students_by_yearly_rank.show()

student_by_yearly_rownumber = avg_score_by_id_year.withColumn("row_number", row_number().over(windowSpec))
student_by_yearly_rownumber.show()



student_by_yearly_dense_rank = avg_score_by_id_year.withColumn("dense_rank",
                                                               dense_rank().over(windowSpec))
student_by_yearly_dense_rank.show()





