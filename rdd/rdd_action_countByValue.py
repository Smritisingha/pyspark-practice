import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
mycol = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(mycol)

countbyvalue = words.countByValue()
print(countbyvalue)

first = words.first()
print(first)
# countByValue() This method counts the number of values in a given RDD.