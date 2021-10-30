from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

data = [
    ("a", "IT", 100),
    ("b", "HR", 200),
    ("c", "SALES", 500),
    ("a1", "IT", 200),
    ("c1", "SALES", 500)
]


def add(sal: int):
    new_sal = sal + (sal * 5) / 100
    return new_sal


add_udf = udf(lambda x: add(x))


def increase_sal(df: DataFrame) -> DataFrame:
    df_new = df.withColumn("sal", add_udf(col("sal").cast(IntegerType())))
    return df_new


def suppress_py4j_logging():
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


def create_testing_pyspark_session():
    return (SparkSession.builder
            .master("local[2]")
            .appName("my-local-testing-pyspark-context")
            .enableHiveSupport()
            .getOrCreate())


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(data, ["name", "dept", "sal"])
    df.show(truncate=False)
    df.printSchema()
    after_sal_inc = increase_sal(df)
    after_sal_inc.show(truncate=False)
