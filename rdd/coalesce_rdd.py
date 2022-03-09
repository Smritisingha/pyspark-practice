import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
a = words.take(10)
print(a, "\n")

partition = words.getNumPartitions()
print(partition)

result = words.coalesce(2).getNumPartitions()
print(result)

repertition = words.repartition(10).getNumPartitions()
print(repertition)

# it can decrease the number of partition but cant increase it
# if we take df.repartition(3) then it'll increase or decrease the partition by 3
# by getNumPartition() get number of partition