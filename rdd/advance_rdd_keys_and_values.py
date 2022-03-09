import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
keyw = words.keyBy(lambda word: word.lower()[0])

keys = keyw.keys().collect()
print(keys) # we can extract the specific keys by using .keys() function

values = keyw.values().collect()
print(values) # we can extract the specific values by using .values() function

