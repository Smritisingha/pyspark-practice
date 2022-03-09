import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

rdd1 = sc.textFile("E:/notepad/justdata.txt")

rdd4 = rdd1.reduceByKey(lambda x, y : x + y)
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey().collect()
for i in rdd5:
    print(i)

#In Spark, the sortByKey function maintains the order of elements.