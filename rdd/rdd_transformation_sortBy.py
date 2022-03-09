import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycol = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(mycol)

sortby = words.sortBy(lambda word: len(word) * -3).collect()

print(sortby)
