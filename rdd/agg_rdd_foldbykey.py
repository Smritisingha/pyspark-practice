import findspark

from pyspark import SparkConf, SparkContext

findspark.init()
from functools import reduce

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
chars = words.flatMap(lambda word: word.lower())
kvchar = chars.map(lambda letter: (letter, 1))


def maxfunc(left, right):
    return max(left, right)


def addfunc(left, right):
    return left + right


foldbykey = kvchar.foldByKey(0, addfunc).collect()
print(foldbykey)

for i in foldbykey:
    print(i)

# foldByKey merge the values for each key  using an associative function and a neutral “zero value,”
# which can be added to the result an arbitrary number of times, and must not change the result