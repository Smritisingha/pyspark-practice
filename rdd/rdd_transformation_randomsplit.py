import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycol = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(mycol)

split = words.randomSplit([0.5, 0.5])

print(split)

#randomSplit()	Splits the RDD by the weights specified in the argument. For example rdd.randomSplit(0.5,0.5)
