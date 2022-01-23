from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Suman Banerjee", 0, [500]),
    (2, "Matei Zaharia", 1, [500, 250, 100]),
    (3, "Michael Armbrust", 1, [250, 100]),
    (4, "Michael Armbrust", 100, [250, 100])]) \
    .toDF("id", "name", "graduate_program", "spark_status")
person.printSchema()
person.show()


graduateProgram = spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")] , ["id", "degree", "department", "school"])

graduateProgram.show()

sparkStatus = spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")]) \
    .toDF("id", "status")

sparkStatus.show()

# 1. inner join // default join
print("person_graduate_prog_inner")
person_graduate_prog_inner = person.join(graduateProgram, person["graduate_program"] == graduateProgram["id"],
                                         "inner").drop(graduateProgram.id)
person_graduate_prog_inner.show(truncate=False)

print("person_graduate_prog_innerNot , find the rec which are not found in persons DF")
person_graduate_prog_innerNot = graduateProgram.join(person, person["graduate_program"] == graduateProgram["id"],
                                                     "left") \
    .filter(person["graduate_program"].isNull())

person_graduate_prog_innerNot.show(truncate=False)

# 2. left join
print("person_graduate_prog_left")
person_graduate_prog_left = person.join(graduateProgram, person["graduate_program"] == graduateProgram["id"], "left")
person_graduate_prog_left.show(truncate=False)

# Not required just try to convert into left join by adjusting required DF
# print("person_graduate_prog_right")
# person_graduate_prog_right = person.join(graduateProgram, person["graduate_program"] == graduateProgram.id , "right")
# person_graduate_prog_right.show(truncate=False)

# 3. left semi :- same as inner join difference is only the data will taken from left DF
# They do not actually include any values from the right DataFrame. They only compare values to see if
# the value exists in the second DataFrame. If the value does exist, those rows will be kept
# in the result, even if there are duplicate keys in the left DataFrame.


print("person_graduate_prog_semi")
person_graduate_prog_semi = person.join(graduateProgram, person["graduate_program"] == graduateProgram.id, "semi")
person_graduate_prog_semi.show(truncate=False)

# 4. left anti :returns data from left DF which are not matched with the right DF
# Left anti joins are the opposite of left semi joins. Like left semi joins, they do not
# actually include any values from the right DataFrame. They only compare values to see
# if the value exists in the second DataFrame. However, in left anti join,
# rather than keeping the values that
# exist in the second DataFrame, they keep only the values that do not have a corresponding
# key in the second DataFrame. Think of anti joins as a NOT IN SQL-style filter
print("person_graduate_prog_anti")
person_graduate_prog_anti = person.join(graduateProgram, person["graduate_program"] == graduateProgram.id, "anti")
person_graduate_prog_anti.show(truncate=False)

# Cross join , n x m records , very expensive

person_graduate_prog_cross = graduateProgram.join(person, person["graduate_program"] == graduateProgram["id"],
                                                  "cross")
person_graduate_prog_cross.show(truncate=False)
