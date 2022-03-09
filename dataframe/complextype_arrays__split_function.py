from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.option("header", "true").csv("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.show()

from pyspark.sql.functions import split, col

df.select(split(col("Team"), " ")).show() # split the values of the column

df.select((split(col("Team"), " ").alias("array_col"))).selectExpr("array_col[0]").show(2) # shows the 0th index value of the column

