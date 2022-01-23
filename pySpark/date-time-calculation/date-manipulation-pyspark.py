from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def create_spark() -> SparkSession:
    """ create spark session instance """
    spark = SparkSession.builder.getOrCreate()
    return spark


data = [(1, "2021-02-01T04:59:00Z")]

df = create_spark().createDataFrame(data, ["id", "utc_date"])

# 1. UTC Timestamp to EST/PST/CST Conversion
df = df.withColumn("date_cet", from_utc_timestamp(col("utc_date"), "CET"))
df = df.withColumn("date_ist", from_utc_timestamp(col("utc_date"), "IST"))

df.show(truncate=False)

# difference between zone based id and zone offset

df = df.withColumn("region_based_zone_id", from_utc_timestamp(col("utc_date"),
                                                              "America/New_York"))
df = df.withColumn("zone_offset",
                   from_utc_timestamp(col("utc_date"), "EST"))
df.show(truncate=False)

# 2. Unix Timestamp to human readable Conversion
data = [(1, -743237207),
        (2, 743237207)]

df = create_spark().createDataFrame(data, ["id", "unix_time_sec"])

# from_unixtime is used to convert unix_timestamp big int to String human
# readable format passed as argument.
df = df.withColumn("unix_time_string", from_unixtime(col("unix_time_sec"), ""
                                                                           "yyyy-MM-dd HH:mm:SS"))

df.show(truncate=False)

# 3 Understand unix_timestamp and from_unixtime
df = df.withColumn("curr_unix_timestamp", unix_timestamp()). \
    withColumn("curr_unix_timestamp_formatted",
               from_unixtime(col("curr_unix_timestamp"),
                             "yyyy-MM-dd HH:mm:SS.S"))
df.show(truncate=False)