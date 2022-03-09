import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
wordread = words.take(10)
print(wordread)
mapfunction = words.map(lambda word: (word.lower(), 1)).collect()
print(mapfunction)

#PySpark map ( map() ) is an RDD transformation that is used to apply the
# transformation function (lambda) on every element of RDD/DataFrame and returns a new RDD.

#map() = return keyvalue pair,transformation is used the apply any complex operations like adding a column,
# updating a column e.t.c, the output of map transformations would always have the same number of records as input.