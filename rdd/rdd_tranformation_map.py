import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycol = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(mycol)

words2 = words.map(lambda word: word[0])

for i in words2.collect():
    print(i)
