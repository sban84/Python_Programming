from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, split, array, struct
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("array_type_example").master("local") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

complex_data = [
    ("A", ["Java", "Scala", "C++" ], ["Java", "Python"], "OH", {"state":"California","code":"CA"}),
    ("B", ["Spark", "Hadoop"], ["Spark"], "NY", {"state":"New Jersey", "code": "NJ"}),
    ("C", ["SQL", "ORACLE", None], ["AWS"], "UT", {"state": "Texas", "code": "TX"})
]

schema = StructType([
    StructField("name", StringType(), False),
    StructField("languagesAtSchool", ArrayType(StringType()), True),
    StructField("languagesAtWork", ArrayType(StringType()), True),
    StructField("currentState", StringType(), False),

    StructField("details", StructType([
        StructField("state", StringType(), True),
        StructField("code", StringType(), True),
    ]))
])

# complex_data_df = spark.createDataFrame(complex_data, ["name", "languagesAtSchool",
#                                                        "languagesAtWork", "currentState",
#                                                        "previousState"])

complex_data_df = spark.createDataFrame(complex_data, schema=schema)
complex_data_df.printSchema()
complex_data_df.show(truncate=False)

complex_data_df.withColumn("langAtSchool_exploded" , explode(col("languagesAtSchool"))).show(truncate=False)

complex_data_df.withColumn("langAtSchool_exploded" , explode_outer(col("languagesAtSchool"))).show(truncate=False)
