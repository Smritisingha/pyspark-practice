import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
mycol = "Spark The Definitive Guide : Big Data".split(" ")
words = sc.parallelize(mycol)
# cache or persist only handle data in memory
cache = words.cache()
print(cache)

getstoragelevel = words.getStorageLevel()
print(getstoragelevel)
# cache and persist only handle data in memory.