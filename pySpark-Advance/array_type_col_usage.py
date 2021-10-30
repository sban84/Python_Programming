from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, split, array, struct
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("array_type_example").master("local") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

complex_data = [
    ("James,Smith", ["Java", "Scala", "C++"], ["Spark", "Java"], "OH", "CA"),
    ("Michael,Rose,", ["Spark", "Java", "C++"], ["Spark", "Java"], "NY", "NJ"),
    ("Robert,Williams", ["CSharp", "VB"], ["Spark", "Python"], "UT", "NV")
]

complex_data_df = spark.createDataFrame(complex_data, ["name", "languagesAtSchool",
                                                       "languagesAtWork", "currentState",
                                                       "previousState"])

complex_data_df.show(truncate=False)

complex_data_df.printSchema()

# 1. array_contains
complex_data_df.select(col("name"), array_contains(col("languagesAtSchool"), "Java")
                       .alias("array_contains")).show()

print("Array filter")
complex_data_df.filter(array_contains(col("languagesAtSchool"), "Java")).show(truncate=False)

# 2. split usage , can be used for a col which has multiple values and we wanted to create diff cols
# based on the separator char
print("Array Split")
complex_data_df.withColumn("f_name", split(col("name"), ",").getItem(0)) \
    .withColumn("l_name", split(col("name"), ",").getItem(1)).show()

# 3. get elements from array types cols and create new col taking individual elements
print('array col split')
complex_data_df.withColumn("langAtSchool_1", col("languagesAtSchool")[0]).withColumn(
    "langAtSchool_2", col("languagesAtSchool")[1]).show()

# 4, create a array type col by taking any other types of col.
# complex_data_df.printSchema()
complex_data_df.withColumn("cities", array(col("currentState"), col("previousState"))) \
    .show()


# 5, create separate  cols by taking elements from array types cols, refer create_array_col.py
# No need to follow this as we can do that as shown in step # 3. get elements from array types cols in this file
# only the diff is here we need to manually getItem(0) for each item to create col for that item
# But if we use create_array_col.py, then a new col with array type will be created then we need to explode that
# so better to use this one mentioned in step #3
# def explode_array_col_custom(colName):
#     return array([struct(col(colName).getItem(0))])
#
#
# complex_data_df.withColumn("langAtSchool_1", explode_array_col_custom("languagesAtSchool")).show()

# 6. Find the common elements between 2 array col type in pyspark

complex_data_df.withColumn("comm_elemts" , array_intersect(col("languagesAtSchool"),col("languagesAtWork"))).\
    show()

