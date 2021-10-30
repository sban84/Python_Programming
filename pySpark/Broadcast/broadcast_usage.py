"""
Below is an example of how to use broadcast variables on DataFrame, similar to above RDD example, This also uses commonly
used data (states) in a Map variable and distributes the variable using SparkContext.broadcast() and
then use these variables on DataFrame map() transformation.

This example following more production ready code style, meaning using functions.
readability and extensions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def getSparkSession():
    spark = SparkSession.builder.appName("SparkExample").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    print("Spark Session created successfully")
    return spark


def getStatesMap():
    return {"KA": "Karnataka", "WB": "West Bengal", "UP": "Utter Pradesh"}


def getData():
    data = [("Sban", "Smith", "IND", "KA"),
            ("Michael", "Rose", "IND", "UP"),
            ("Robert", "Williams", "IND", "WB"),
            ("Maria", "Jones", "USA", "FL")
            ]

    return data


def getStateName(s):
    states_map = getStatesMap()
    return states_map.get(s)


get_states_names = udf(lambda x: getStateName(x), StringType())


def main():
    spark = getSparkSession()
    print("Inside main func sparkSession: ", spark)
    df = spark.createDataFrame(getData(), schema=["f_name", "l_name", "country", "state"])
    df.show(truncate=False)
    bc = spark.sparkContext.broadcast(getStatesMap())
    df = df.withColumn("state_name", get_states_names(col("state").cast(StringType())))
    df.show(truncate=False)

    # below code is just to iterate the df.rdd and print each rows
    df.rdd.foreach(lambda x: print(x))


# print("The value of __name__ is:", repr(__name__))
if __name__ == "__main__":
    main()


