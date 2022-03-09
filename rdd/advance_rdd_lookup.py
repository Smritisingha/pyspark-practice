import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
keyw = words.keyBy(lambda word: word.lower()[0])
lookup = keyw.lookup("s")
print(lookup)

# lookup give the result for particular key # like we lookup("s") #get spark, simple
