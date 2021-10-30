# Very Very useful code to understand how to create complex data using array, struct
# and how to flatten and process the data , refer this

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, LongType

from flatten_df import flatten_complex_df

spark = SparkSession.builder.appName("Complex_DataType_Understand").master("local") \
    .getOrCreate()
# challenge 1 , below code is very useful to see how we can create struct type in DF manually
# and how to process / access
data = [
    (1, {"name": "India", "capital": "Delhi"}, "INR"),
    (2, {'name': 'Italy', 'capital': 'Rome'}, 'euro'),
    (3, {'name': 'France', 'capital': 'Paris'}, 'euro'),
    (4, {'name': 'Japan', 'capital': 'Tokyo'}, 'yen')
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("country", StructType([
        StructField("name", StringType(), True),
        StructField("capital", StringType(), True),
    ]), True),
    StructField("currency", StringType(), True),
])

# complex_df = spark.createDataFrame(data, ["id", "country", "currency"])
complex_df = spark.createDataFrame(data, schema=schema)

complex_df.printSchema()
# root
# |-- id: integer (nullable = false)
# |-- country: struct (nullable = true)
# |    |-- name: string (nullable = true)
# |    |-- capital: string (nullable = true)
# |-- currency: string (nullable = true)
complex_df.show(truncate=False)

# complex_df.filter(complex_df["id"] == 1) # way but remember the col way of doing for most of the cases
complex_df.filter(col("country.name") == "India").show(truncate=False)

# End of challenge 1
print("\n######### End of challenge 1 ############")
# data = [
#     (1, {"name": "India", "capital": "Delhi", "other_cities": ["Bangalore", "Kolkata"]}, "INR"),
#     (2, {'name': 'Italy', 'capital': 'Rome', "other_cities": ["Milan"]}, 'euro'),
#     (3, {'name': 'France', 'capital': 'Paris', "other_cities": ["Paris"]}, 'euro'),
#     (4, {'name': 'Japan', 'capital': 'Tokyo', "other_cities": []}, 'yen')
# ]

my_new_schema = StructType([
    StructField('id', LongType()),
    StructField('countries', ArrayType(StructType([
        StructField('name', StringType()),
        StructField('capital', StringType()),
        StructField('other_cities', ArrayType(StringType()), True)
    ]))),
    StructField("details", StructType([
        StructField("total_population", StringType(), True),
        StructField("area", IntegerType(), True),
    ]))
]
)
struct_with_array = [
    (1, [
        {'name': 'Italy', 'capital': 'Rome', "other_cities": ["Milan"]},
        {'name': 'Spain', 'capital': 'Madrid', "other_cities": []}
    ], {"total_population": "100 million", "area": 20}
     ),
    (2, [
        {'name': 'Japan', 'capital': 'Tokyo', "other_cities": ["Osaka"]},
        {'name': 'India', 'capital': 'Delhi', "other_cities": ["Bangalore", "Kolkata"]}
    ], {"total_population": "1000 million", "area": 200}
     )
]

complex_df = spark.createDataFrame(struct_with_array, schema=my_new_schema)
complex_df.printSchema()
complex_df.show(truncate=False)

print(complex_df.schema)
flatten_complex_df(complex_df).show(truncate=False)

df = spark.read.option("multiline", "true").json("./nested_data.json")
flatten_complex_df(df).show(truncate=False)

df = flatten_complex_df(df)

df.selectExpr("*", "initcap(custom_dimensions_customerInfo_CustomerName) as Title_CustomerName").select(
    col("Title_CustomerName")).show(3)

df.withColumn("custom_dimensions_customerInfo_UserName",
              regexp_replace(col("custom_dimensions_customerInfo_UserName"), "XXXX", "YYYY")).show(3)


