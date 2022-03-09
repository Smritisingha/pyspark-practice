import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
chars = words.flatMap(lambda word: word.lower())
kvchar = chars.map(lambda letter: (letter, 1))


def valtocombiner(value):
    return [value]


def mergevalfunc(vals, valToAppend):
    vals.append(valToAppend)
    return vals


def mergecombinerfunc(vals1, vals2):
    return vals1 + vals2


outputpertition = 6

combinebykey = kvchar.combineByKey(valtocombiner, mergevalfunc, mergecombinerfunc, outputpertition).collect()
print(combinebykey)


# Generic function to combine the elements for each key using a custom set of aggregation functions.
# Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a “combined type” C.
#Instead of specifying an aggregation function, you can specify a combiner. This combiner
# operates on a given key and merges the values according to some function