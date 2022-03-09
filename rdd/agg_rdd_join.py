import findspark

from pyspark import SparkConf, SparkContext
import random

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
chars = words.flatMap(lambda word: word.lower())
kvchar = chars.map(lambda letter: (letter, 1))
dist = words.flatMap(lambda word: word.lower()).distinct()
keyedchars = dist.map(lambda c: (c, random.random()))

outputpertition = 10
join = kvchar.join(keyedchars).count()
join1 = kvchar.join(keyedchars, outputpertition).count()
print(join)
print(join1)

numrange = sc.parallelize(range(10), 4)
h = words.zip(numrange).collect()

# the two RDDs we would like to join, and,
# optionally, either the number of output partitions or the customer partition function to which they
# should output. W