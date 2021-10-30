from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("MyApp") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
#
# default_conf = spark.sparkContext._conf.getAll()
# print(default_conf)

conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '4g'),
                                        ('spark.app.name', 'Spark Updated Conf'),
                                        ('spark.executor.cores', '4'),
                                        ('spark.cores.max', '4'),
                                        ('spark.driver.memory','4g')])

spark.sparkContext.stop()

spark = SparkSession \
    .builder \
    .appName("MyApp") \
    .config(conf=conf) \
    .getOrCreate()


default_conf = spark.sparkContext._conf.get("spark.cores.max")
print("updated configs " , default_conf)

