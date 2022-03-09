import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

reduce = sc.parallelize(range(1, 21)).reduce(lambda x, y: x + y)
print(reduce)

#“reduce” an RDD of any kind of value to one value. you can reduce this to its sum by specifying a function that
# takes as input two values and reduces them into one.
