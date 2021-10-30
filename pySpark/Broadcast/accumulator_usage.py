"""The PySpark Accumulator is a shared variable that is used with RDD and DataFrame to perform sum and counter operations similar to Map-reduce counters. These variables are shared by all executors to update and add information through
aggregation or commutative operations.

Accumulators are write-only and initialize once variables where only tasks that are running on workers are allowed to
update and updates from the workers get propagated automatically to the driver program. But,
only the driver program is allowed to access the Accumulator variable using the value property.

Using accumulator() from SparkContext class we can create an Accumulator in PySpark programming. Users can also create
Accumulators for custom types using AccumulatorParam class of PySpark.
"""
from pyspark import Accumulator
from pyspark.sql import SparkSession
import numpy as np

# from pyspark.sql.types import *
# from pyspark.sql.functions import *
from pyspark.sql.functions import col, count, when


def getSparkSession():
    spark = SparkSession.builder.appName("SparkExample").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    print("Spark Session created successfully")
    return spark


def getData():
    data = [("Sban", "Smith", "IND", "KA"),
            (None, "Rose", "IND", "UP"),
            (None, "Williams", "IND", "WB"),
            ("sban", "Jones", "USA", "FL")
            ]

    return data


spark = getSparkSession()
df = spark.createDataFrame(getData(), schema=["f_name", "l_name", "country", "state"])
accum = spark.sparkContext.accumulator(0)


# for example we need to count the number of rec with name = null using Accumulator

def check_name_null(c: str):
    if c is None:  # null is None in python
        global accum
        accum.add(1)


df.rdd.foreach(lambda x: print(x))
df.show()
df.rdd.foreach(lambda x: check_name_null(x[0]))
print("accum.value", accum.value)


# Below code is just for practise , to find out the null count for each col. and dropna
mask = [count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]
df.select(mask).show()


df = df.dropna(how="any", subset=["f_name"])
df.show()
