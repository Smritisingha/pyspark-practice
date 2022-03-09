from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("range").getOrCreate()

h = spark.range(10).toDF("id")
for i in h.collect(): print(i) 