import findspark
findspark.init()
from pyspark import SparkConf
from pyspark import SparkContext
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

rdd = sc.parallelize([1, 2, 4, 5])
rdd2 = rdd.filter(lambda x: x % 2 == 0)
for i in rdd2.collect():
    print(i)