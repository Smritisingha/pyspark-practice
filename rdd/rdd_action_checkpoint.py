import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
mycol = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(mycol)
# checkpointing is the act of saving an RDD to disk
checkpoint = words.checkpoint()
print(checkpoint)
#Checkpointing is the act of saving an RDD to disk so that future references to this RDD point to those intermediate
# partitions on disk rather than recomputing the RDD from its original source