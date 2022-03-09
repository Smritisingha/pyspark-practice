import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)

supdata = {"Spark": 1000, "Definitive": 200, "Big": 300, "simple": 100}
broadcast = sc.broadcast(supdata)
value = broadcast.value
# print(broadcast)
print(value)

mapp = words.map(lambda word: (word, value.get(word, 0))).sortBy(lambda wordpair: wordpair[1]).collect()
print(mapp)

