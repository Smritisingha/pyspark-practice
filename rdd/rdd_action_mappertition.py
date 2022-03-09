import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
mycol = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(mycol)
mappertition = words.mapPartitions(lambda part: [1]).sum()
print(mappertition)

# Return a new RDD by applying a function to each partition of this RDD.

