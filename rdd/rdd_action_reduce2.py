import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
mycol = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(mycol)


def wordlength(leftword, rightword):
    if len(leftword) > len(rightword):
        return leftword
    else:
        return rightword


reduce = words.reduce(wordlength)
print(reduce)
