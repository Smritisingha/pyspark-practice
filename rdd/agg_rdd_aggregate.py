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


nums = sc.parallelize(range(1, 31), 5)

aggregate = nums.aggregate(0, maxfunc, addfunc)
print("aggregate :", aggregate)

depth = 3
treeagg = nums.treeAggregate(0, maxfunc, addfunc, depth)
print("treeaggregate :", treeagg)
# it is same as aggregation function, but it does it by key
aggbykey = kvchar.aggregateByKey(0, maxfunc, addfunc).collect()
print("aggregateByKey :", aggbykey)

#Function aggregateByKey is one of the aggregate function (Others are reduceByKey & groupByKey) available in Spark.
# This is the only aggregation function which allows multiple type of aggregation(Maximun, minimun, average, sum & count)
# at the same time.