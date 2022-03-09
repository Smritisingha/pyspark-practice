from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df1 = spark.read.csv("C:/Users/smrit/Downloads/orders.txt", header=True)

df1.show()
