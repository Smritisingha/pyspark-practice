from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.option("header", "true").csv("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.show()

from pyspark.sql.functions import size, col, split, array_contains

df.select(size(split(col("Team"), " "))).show() # show the number of values . for Boston Celtics output return 2

df.select(array_contains(split(col("Team"), " "),"Boston")).show() # we can see whether this array comtains a values or not it gives true or false

