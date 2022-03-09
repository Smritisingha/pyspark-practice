from pyspark import SparkContext
sc = SparkContext.getOrCreate()
collect_rdd = sc.parallelize([1,2,3,4,5])
print(collect_rdd.collect())

count_rdd = sc.parallelize([1,2,3,4,5,5,6,7,8,9])
print(count_rdd.count())
#print(count_rdd.count())
#for i in count_rdd.count():
 #   print(i)