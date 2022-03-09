import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
chars = words.flatMap(lambda word: word.lower())
kvchar = chars.map(lambda letter: (letter, 1))


def maxfunc(left, right):
    return max(left, right)


def addfunc(left, right):
    return left + right


reducebykey = kvchar.reduceByKey(addfunc).collect()
print(reducebykey)

#  Merge the values for each key using an associative and commutative reduce function.