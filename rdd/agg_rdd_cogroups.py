import findspark

from pyspark import SparkConf, SparkContext
import random

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)

distinctchar = words.flatMap(lambda word: word.lower()).distinct()
charrdd = distinctchar.map(lambda c: (c, random.random()))
charrdd2 = distinctchar.map(lambda c: (c, random.random()))
cogroup = charrdd.cogroup(charrdd2).collect()
print(cogroup)

# cogroup gives you the ability to group together up to two key - values rdd in python
# this joins the given values by key.
# this is effectively just a group based join on an rdd