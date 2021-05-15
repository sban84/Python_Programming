from pyspark.sql.functions import first
from pyspark.sql import SparkSession
l =[( 1        ,'A', 10, 'size' ),
( 1        , 'A', 30, 'height' ),
( 1        , 'A', 20, 'weigth' ),
( 2        , 'A', 10, 'size' ),
( 2        , 'A', 30, 'height' ),
( 2        , 'A', 20, 'weigth' ),
( 3        , 'A', 10, 'size' ),
( 3        , 'A', 30, 'height' ),
( 3        , 'A', 20, 'weigth' )]
spark = SparkSession.builder.appName("Ref").master("local").getOrCreate()
df = spark.createDataFrame(l, ['id','place', 'value', 'attribute'])
# +---+-----+------+----+------+
# # | id|place|height|size|weigth|
# # +---+-----+------+----+------+
# # |  2|    A|    30|  10|    20|
# # |  3|    A|    30|  10|    20|
# # |  1|    A|    30|  10|    20|
# # +---+-----+------+----+------+
df.groupBy(df.id, df.place).pivot('attribute').agg(first("value")).show()

