from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, LongType

spark = SparkSession.builder.appName("Complex_df").master("local") \
    .getOrCreate()

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
exp = complex_df.withColumn("exploded", explode_outer(col("countries")))
exp.show(truncate=False)


# BELOW CODE IS NOT IMPORTANT AT THE MOMENT
# def col_analysis(df: DataFrame):
#     # print(df.schema.fields)
#     df.show(truncate=False)
#     filed_name_type_dict = \
#         dict([(field.name, field.dataType) for field in df.schema.fields if isinstance(field.dataType, ArrayType)
#               | isinstance(field.dataType, StructType)])
#
#     for key, v in filed_name_type_dict.items():
#         if len(filed_name_type_dict) == 0:
#             print("inside filed_name_type_dict == 0 ")
#             break
#
#         qualify = list(filed_name_type_dict.keys())[0] + "_"
#         if isinstance(v, StructType):
#             print("inside struct for col ", key)
#             # print([n for n in v])
#             exploded_struct_col = [col(key + '.' + k).alias(key + '_' + k) for k in [n.name for n in v]]
#             print(exploded_struct_col)
#             df = df.select("*", *exploded_struct_col).drop(key)
#             df.show(truncate=False)
#
#         elif isinstance(v, ArrayType):
#             print("inside ArrayType", key)
#             df = df.withColumn(key, explode_outer(col(key)))
#
#         print("after each iteration ")
#         df.printSchema()
#         col_analysis(df)
#     return df
#
#     # for df_col_name in df.columns:
#     #     df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))
#
#
# col_analysis(complex_df).show(truncate=False)
