import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
# glom convert partition to an array
glom1 = sc.parallelize(["Hello", "World"], 2).glom().collect()
print(glom1)

num = sc.parallelize([5, 6, 4, 7, 8, 9, 1, 2, 3, 0])
glom = num.glom().collect()
print(glom)
