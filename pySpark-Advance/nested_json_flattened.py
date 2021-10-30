# refernce : https://docs.microsoft.com/en-us/azure/synapse-analytics/how-to-analyze-complex-schema
# https://gist.github.com/nmukerje/e65cde41be85470e4b8dfd9a2d6aed50
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import flatten
from pyspark.sql.functions import arrays_zip

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.option("multiline", "true").json("./nested_data.json")
df.show(truncate=False)


def flatten_df(nested_df):
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)


from pyspark.sql.types import StringType, StructField, StructType

df_flat = flatten_df(df)
df_flat.show(truncate=False)
df.printSchema()
df_flat.printSchema()

df_flat_explode = df_flat.select( explode(df_flat.context_custom_dimensions),
                                 "context_session_isFirst", "context_session_id", "context_data_eventTime",
                                 "context_data_samplingRate", "context_data_isSynthetic")

df_flat_explode.show(truncate=False)
df_flat_explode_flat = flatten_df(df_flat_explode)
df_flat_explode_flat.show(truncate=False)


