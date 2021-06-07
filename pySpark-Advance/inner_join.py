"""
Inner join example
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

emp = [(1, "Smith", -1, "2018", "10", "M", 3000), \
       (2, "Rose", 1, "2010", "20", "M", 4000), \
       (3, "Williams", 1, "2010", "10", "M", 1000), \
       (4, "Jones", 2, "2005", "10", "F", 2000), \
       (5, "Brown", 2, "2010", "40", "", -1), \
       (6, "Brown", 2, "2010", "50", "", -1) \
       ]
empColumns = ["emp_id", "name", "superior_emp_id", "year_joined",
              "emp_dept_id", "gender", "salary"]

emp_df = spark.createDataFrame(data=emp, schema=empColumns)
emp_df.show()

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
deptColumns = ["dept_name", "dept_id"]

dept_df = spark.createDataFrame(dept , deptColumns)
dept_df.show()

comon_rec = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "inner")
comon_rec.show(truncate=False)

full_rec = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "outer") # all data from both df
full_rec.show(truncate=False)

left_join  = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "left")
left_join.show(truncate=False)

# Left Semi Join :- leftsemi join is similar to inner join difference being leftsemi join returns all columns from
# the left dataset and ignores all columns from the right dataset. In other words, this join returns columns from the
# only left dataset for the records match in the right dataset on join expression, records not matched on join
# expression are ignored from both left and right datasets.

left_semi_join  = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "leftsemi")
left_semi_join.show(truncate=False)


# Left Anti Join :- leftanti join does the exact opposite of the leftsemi, leftanti join returns only columns from
# the left dataset for non-matched records.

left_anti_join  = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "leftanti")
left_anti_join.show(truncate=False)

# Self Join , very useful for some cases , like here to find the employees who are supervisor

supervisor_emp_details = emp_df.alias("emp").join(emp_df.alias("manager"), col("emp.emp_id")
                                                  == col("manager.superior_emp_id") , "inner")\
       .select(col("emp.emp_id"),col("emp.name"),col("emp.gender"),col("emp.salary"))\
       .dropDuplicates(["emp_id"])
#supervisor_emp_details.printSchema()
supervisor_emp_details.show(truncate=False)

