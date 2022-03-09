from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()


# Create RDD from parallelize
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
# print(rdd.count())


# Create RDD from external Data source
rdd2 = spark.sparkContext.textFile("C:/Users/smrit/Downloads/orders.txt")
print(rdd2.collect())
for i in rdd2.collect():
    print(i)

# Reads entire file into a RDD as single record.
#rdd3 = spark.sparkContext.wholeTextFiles("C:/Users/smrit/Downloads/New folder")
#print(rdd3)
