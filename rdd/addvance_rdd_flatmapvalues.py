import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
keyw = words.keyBy(lambda word: word.lower()[0])
flatmapvalue = keyw.flatMapValues(lambda word: word.upper()).collect()
# print == column wise
print(flatmapvalue)

for i in flatmapvalue:
    print(i)

# for loop prints in row wise
#flatmapvalues Pass each value in the key-value pair RDD through a flatMap function without changing the keys;