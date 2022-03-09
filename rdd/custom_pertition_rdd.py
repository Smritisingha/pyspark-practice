from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/smrit/Downloads/orders.txt")
rdd = df.coalesce(10).rdd.collect()
print(rdd)
df.printSchema()
