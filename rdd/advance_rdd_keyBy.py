import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)

keyw = words.keyBy(lambda word: word.lower()[0]).collect()
print(keyw)

# Creates tuples of the elements in this RDD by applying KeyBy function . you are
# keying by the first letter in the word . spark then keeps the record
# as the value of the keyed rdd .
