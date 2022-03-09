import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

# Create RDD from parallelize
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = sc.parallelize(data)
print(rdd.collect())
r = "hello  world".split(" ")
s = sc.parallelize(r)
r1 = sc.parallelize(range(1, 20))

rdd2 = rdd.map(lambda x: (x, 1))
print(rdd2.collect())

rdd3 = r1.reduce(lambda a, b: a+b)
print(rdd3)