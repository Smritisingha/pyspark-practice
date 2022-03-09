import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycol = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(mycol)

flatmap = words.flatMap(lambda word: list(word)).take(5)
print(flatmap)

#flattens the RDD/DataFrame (array/map DataFrame columns) after applying
# the function on every element and returns a new PySpark RDD/DataFrame.
