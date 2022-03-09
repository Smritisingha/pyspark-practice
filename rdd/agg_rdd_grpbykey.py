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


nums = sc.parallelize(range(1, 40), 5)

groupbykey = kvchar.groupByKey().map(lambda row: (row[0], reduce(addfunc, row[1]))).collect()
print(groupbykey)

for i in groupbykey:
    print(i)

# groupbykey with a map over each grouping is the
# best way to sum up the counts for each key: