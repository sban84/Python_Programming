import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# conf = SparkConf().setAppName("Test").setMaster("local")
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Test").master("local").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

l = [1, 2, 3, 4, 5]
data = sc.parallelize(l, 2)
data.getNumPartitions()
data.collect()
print(data.filter(lambda x: x % 2 == 0))


def getEven(n):
    return n % 2 == 0


even = data.filter(lambda x: getEven(x))
print(even.collect())

data_sum = data.reduce(lambda x, y: x + y)
print(f" sum = {data_sum}")

## word count in Spark , rdd way

string = "this is a spark code words count, a we need to count words by using spark code in spark a"
string_list = string.split(" ")
rdd = spark.sparkContext.parallelize(string_list , 2)
print(rdd.collect())

# sort by key desc order , if the words are not in flattened , then we need to use flatMap(lambda x: x)

words_with_count = rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y : x + y).sortByKey(ascending=False)
print(words_with_count.collect())

# sort by values desc order
words_with_count = rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y : x + y).sortBy(lambda x: x[1],ascending=False)
print(words_with_count.collect())

data =[ "Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s" ]
rdd = spark.sparkContext.parallelize(data)
words_with_count =  rdd.flatMap(lambda x: x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1],ascending=False
                                                                                                                )

print(words_with_count.collect() )


### Using DF the same
words = rdd.flatMap(lambda x: x.split(" "))
df = spark.createDataFrame(words , StringType()).toDF("word")
# order by ny multiple cols , if first col has tie then sec order by will happen by sec col according to the ascending rules

df = df.groupBy(col("word")).count().alias("count").orderBy([col("count"),col("word")] , ascending=[False,False])
df.show()

