from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)

from pyspark.sql.functions import lit,expr

print(df.select("Name","Team").distinct().count())

print(df.select("Team").distinct().count())
