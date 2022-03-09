import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
keyw = words.keyBy(lambda word: word.lower()[0])
# Creates tuples of the elements in this RDD by applying KeyBy function . you are
# keying by the first letter in the word . spark then keeps the record
# as the value of the keyed rdd .

mapvalues = keyw.mapValues(lambda word: word.upper()).collect()
print(mapvalues)

for i in mapvalues:
    print(i)
# if you have tupple spark will assume the first element is key
#mapValues() Pass each value in the key-value pair RDD through a map function without changing the keys

