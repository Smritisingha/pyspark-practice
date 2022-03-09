from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()

# Create empty RDD with partition
rdd2 = spark.sparkContext.parallelize([], 10) # This creates 10 partitions

print(rdd2.collect())


reparRdd = rdd2.repartition(4)
print("re-partition count:"+str(reparRdd.getNumPartitions()))
#Outputs: "re-partition count:4
