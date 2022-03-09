import findspark

from pyspark import SparkConf, SparkContext

findspark.init()
# from functools import reduce

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
chars = words.flatMap(lambda word: word.lower())
kvchar = chars.map(lambda letter: (letter, 1))
kv1 = words.map(lambda word: (word.upper()[0], word)).collect()
kv2 = words.flatMap(lambda word: (word.upper()[0], word)).collect()
char = chars.collect()
print(char, "\n")
kv = kvchar.collect()
print("map", kv1)  # maps gives you output in tuples
print("fmap", kv2) # flatmap gives you output in flatten

