from pyspark import SparkConf, SparkContext
import findspark

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

col = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(col)

for i in words.collect():
    print(i)

words.collect()
# count the number of rows in rdd
words.count()

print(words.setName("mywords"))
print(words.name())
